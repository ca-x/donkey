param(
  [ValidateSet('configure', 'temporary', 'check', 'restore')]
  [string]$Action = 'configure',
  [string]$Url = $env:DONKEY_URL,
  [string]$Username = $env:DONKEY_USERNAME,
  [string]$Backup,
  [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

function Normalize-DonkeyUrl {
  if ([string]::IsNullOrWhiteSpace($Url)) { throw 'Missing -Url' }
  if ($Url -notmatch '^https?://') { $script:Url = "https://$Url" }
  $script:Url = $Url.TrimEnd('/')
}

function Get-RegistryHost { return ([Uri]$Url).Authority }

function Get-SettingsPath {
  $store = Join-Path $env:APPDATA 'Docker\settings-store.json'
  $legacy = Join-Path $env:APPDATA 'Docker\settings.json'
  if (Test-Path $store) { return $store }
  return $legacy
}

function Merge-RegistryMirror([string]$Path) {
  $config = @{}
  if (Test-Path $Path) {
    $raw = Get-Content -Raw -LiteralPath $Path
    if (-not [string]::IsNullOrWhiteSpace($raw)) {
      $parsed = $raw | ConvertFrom-Json
      foreach ($property in $parsed.PSObject.Properties) {
        $config[$property.Name] = $property.Value
      }
    }
  }
  $mirrors = @($config.registryMirrors)
  if ($mirrors -notcontains $Url) { $mirrors = @($Url) + $mirrors }
  $config.registryMirrors = $mirrors
  $json = $config | ConvertTo-Json -Depth 32
  if ($DryRun) { Write-Host "Would write ${Path}:`n$json"; return }
  $directory = Split-Path -Parent $Path
  New-Item -ItemType Directory -Force -Path $directory | Out-Null
  if (Test-Path $Path) {
    $backupPath = "$Path.donkey.$(Get-Date -Format yyyyMMddHHmmss).bak"
    Copy-Item -LiteralPath $Path -Destination $backupPath
    Write-Host "Backup: $backupPath"
  }
  $temporary = "$Path.donkey.tmp"
  Set-Content -LiteralPath $temporary -Value $json -Encoding utf8
  $null = Get-Content -Raw -LiteralPath $temporary | ConvertFrom-Json
  Move-Item -LiteralPath $temporary -Destination $Path -Force
  Write-Host "Updated: $Path"
}

function Restart-DockerDesktop {
  if ($DryRun) { return }
  try {
    docker desktop restart
  } catch {
    Write-Warning 'Restart Docker Desktop manually to apply the new mirror configuration.'
  }
}

switch ($Action) {
  'configure' {
    Normalize-DonkeyUrl
    Merge-RegistryMirror (Get-SettingsPath)
    Restart-DockerDesktop
    if (-not [string]::IsNullOrWhiteSpace($Username) -and -not $DryRun) {
      $password = Read-Host "Password for $Username" -AsSecureString
      $plain = [System.Net.NetworkCredential]::new('', $password).Password
      $plain | docker login (Get-RegistryHost) --username $Username --password-stdin
      $plain = $null
    }
  }
  'temporary' {
    Normalize-DonkeyUrl
    $hostName = Get-RegistryHost
    Write-Host "docker login $hostName"
    Write-Host "docker pull $hostName/library/alpine:latest"
  }
  'check' {
    docker version
    docker info
  }
  'restore' {
    if ([string]::IsNullOrWhiteSpace($Backup) -or -not (Test-Path $Backup)) { throw 'restore requires an existing -Backup file' }
    $target = Get-SettingsPath
    if ($DryRun) { Write-Host "Would restore $Backup to $target"; break }
    Copy-Item -LiteralPath $Backup -Destination $target -Force
    Restart-DockerDesktop
    Write-Host "Restored: $target"
  }
}
