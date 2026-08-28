use std::{
    net::IpAddr,
    sync::{Arc, LazyLock},
};

use ipnet::IpNet;
use reqwest::{Client, ClientBuilder};
use tokio::net::lookup_host;
use url::Url;

use crate::{config::Config, error::AppError};

#[derive(Clone, Debug)]
pub struct ValidatedUpstream {
    pub url: Url,
    pub addresses: Arc<[IpAddr]>,
}

pub async fn validate_upstream(raw: &str, config: &Config) -> Result<ValidatedUpstream, AppError> {
    validate_url(raw, config, true).await
}

pub async fn validate_target_url(
    raw: &str,
    config: &Config,
) -> Result<ValidatedUpstream, AppError> {
    validate_url(raw, config, false).await
}

/// Validate the optional connection override. It may be a literal IP address
/// or a DNS hostname; URL syntax, ports, and credentials are deliberately not
/// accepted here because the upstream URL remains the authority/SNI.
pub fn validate_connect_target_syntax(raw: &str, target_type: &str) -> Result<(), AppError> {
    let value = raw.trim();
    if value.is_empty() || value.len() > 253 || value.chars().any(char::is_whitespace) {
        return Err(AppError::bad_request(
            "connect_ip must be an IP address or hostname",
        ));
    }
    if value.parse::<IpAddr>().is_ok() {
        if target_type != "ip" {
            return Err(AppError::bad_request(
                "connect_ip_type does not match target",
            ));
        }
        return Ok(());
    }
    if target_type != "domain" {
        return Err(AppError::bad_request(
            "connect_ip_type does not match target",
        ));
    }
    let host = value.strip_suffix('.').unwrap_or(value);
    if host.is_empty()
        || host.contains(['/', ':', '@', '[', ']', '?', '#', '%'])
        || host.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err(AppError::bad_request(
            "connect_ip must be an IP address or hostname",
        ));
    }
    Ok(())
}

/// Resolve a connection override and enforce the same public-address policy
/// used for upstream URL validation. All returned addresses are pinned into
/// the client so a DNS answer cannot change mid-request.
pub async fn resolve_connect_target(
    raw: &str,
    target_type: &str,
    port: u16,
    config: &Config,
) -> Result<Arc<[IpAddr]>, AppError> {
    validate_connect_target_syntax(raw, target_type)?;
    let value = raw.trim();
    let addresses = if let Ok(ip) = value.parse::<IpAddr>() {
        vec![ip]
    } else {
        let host = value
            .strip_suffix('.')
            .unwrap_or(value)
            .to_ascii_lowercase();
        lookup_host((host.as_str(), port))
            .await
            .map_err(|_| AppError::bad_request("connect_ip DNS lookup failed"))?
            .map(|address| address.ip())
            .collect()
    };
    let mut unique = Vec::with_capacity(addresses.len());
    for ip in addresses {
        if !config.allow_private_upstreams && is_non_public(ip) {
            return Err(AppError::bad_request("private connect_ip is disabled"));
        }
        if !unique.contains(&ip) {
            unique.push(ip);
        }
    }
    if unique.is_empty() {
        return Err(AppError::bad_request(
            "connect_ip DNS returned no addresses",
        ));
    }
    Ok(unique.into())
}

async fn validate_url(
    raw: &str,
    config: &Config,
    normalize_base: bool,
) -> Result<ValidatedUpstream, AppError> {
    if raw.len() > 2048 {
        return Err(AppError::bad_request("upstream URL is too long"));
    }
    let mut url = Url::parse(raw).map_err(|_| AppError::bad_request("invalid upstream URL"))?;
    if url.username() != "" || url.password().is_some() {
        return Err(AppError::bad_request(
            "credentials are not allowed in upstream URLs",
        ));
    }
    match url.scheme() {
        "https" => {}
        "http" if config.allow_insecure_upstreams => {}
        _ => return Err(AppError::bad_request("upstream must use HTTPS")),
    }
    let host = url
        .host_str()
        .ok_or_else(|| AppError::bad_request("upstream URL has no host"))?
        .to_ascii_lowercase();
    url.set_fragment(None);
    if normalize_base {
        url.set_query(None);
        if !url.path().ends_with('/') {
            let next = format!("{}/", url.path());
            url.set_path(&next);
        }
    }

    let port = url
        .port_or_known_default()
        .ok_or_else(|| AppError::bad_request("upstream has no usable port"))?;
    let mut addresses = Vec::new();
    for addr in lookup_host((host.as_str(), port))
        .await
        .map_err(|_| AppError::bad_request("upstream DNS lookup failed"))?
    {
        let ip = addr.ip();
        if !config.allow_private_upstreams && is_non_public(ip) {
            return Err(AppError::bad_request(
                "private or reserved upstream addresses are disabled",
            ));
        }
        if !addresses.contains(&ip) {
            addresses.push(ip);
        }
    }
    if addresses.is_empty() {
        return Err(AppError::bad_request("upstream DNS returned no addresses"));
    }
    Ok(ValidatedUpstream {
        url,
        addresses: addresses.into(),
    })
}

pub fn client_for(
    upstream: &ValidatedUpstream,
    timeout: std::time::Duration,
) -> Result<Client, AppError> {
    let host = upstream
        .url
        .host_str()
        .ok_or_else(|| AppError::bad_request("upstream URL has no host"))?;
    let port = upstream.url.port_or_known_default().unwrap_or(443);
    let socket_addrs = upstream
        .addresses
        .iter()
        .map(|ip| std::net::SocketAddr::new(*ip, port))
        .collect::<Vec<_>>();

    ClientBuilder::new()
        .timeout(timeout)
        .connect_timeout(timeout.min(std::time::Duration::from_secs(10)))
        .redirect(reqwest::redirect::Policy::none())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .resolve_to_addrs(host, &socket_addrs)
        .user_agent(concat!("donkey/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(AppError::internal)
}

pub fn is_non_public(ip: IpAddr) -> bool {
    if let IpAddr::V6(ipv6) = ip
        && let Some(ipv4) = ipv6.to_ipv4_mapped()
    {
        return is_non_public(IpAddr::V4(ipv4));
    }
    DENIED_NETWORKS.iter().any(|network| network.contains(&ip))
}

static DENIED_NETWORKS: LazyLock<Vec<IpNet>> = LazyLock::new(|| {
    [
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
        "::/128",
        "::1/128",
        "64:ff9b:1::/48",
        "100::/64",
        "2001::/23",
        "2001:db8::/32",
        "2002::/16",
        "fc00::/7",
        "fe80::/10",
        "ff00::/8",
    ]
    .into_iter()
    .map(|value| value.parse().expect("hard-coded CIDR must be valid"))
    .collect()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_private_and_metadata_addresses() {
        for value in [
            "127.0.0.1",
            "10.0.0.1",
            "100.64.0.1",
            "169.254.169.254",
            "198.18.0.1",
            "::1",
            "100::1",
            "2001:db8::1",
            "fd00::1",
        ] {
            assert!(is_non_public(value.parse().unwrap()), "{value}");
        }
        assert!(!is_non_public("1.1.1.1".parse().unwrap()));
    }

    #[test]
    fn validates_ip_and_dns_target_types() {
        assert!(validate_connect_target_syntax("1.1.1.1", "ip").is_ok());
        assert!(validate_connect_target_syntax("saas.sin.fan", "domain").is_ok());
        assert!(validate_connect_target_syntax("saas.sin.fan", "ip").is_err());
        assert!(validate_connect_target_syntax("1.1.1.1", "domain").is_err());
        assert!(validate_connect_target_syntax("https://example.com", "domain").is_err());
    }

    #[tokio::test]
    async fn rejects_private_dns_override() {
        let mut config = Config::for_test(std::env::temp_dir());
        config.allow_private_upstreams = false;
        let error = resolve_connect_target("localhost", "domain", 443, &config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("private connect_ip"));
    }
}
