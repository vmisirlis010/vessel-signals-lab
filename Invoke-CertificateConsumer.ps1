param(
    [string]$IdentityPath = (Join-Path $PSScriptRoot 'consumer-identity.json'),
    [string]$ConfigPath = (Join-Path $PSScriptRoot 'consumer-config.json'),
    [ValidateSet('check', 'sync')][string]$Operation = 'check',
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'

if ($Operation -eq 'sync' -and -not $Apply) {
    Write-Output 'Plan: authenticate with an existing certificate and write local SQLite. Add -Apply after consent.'
    return
}

$identity = Get-Content -LiteralPath $IdentityPath -Raw | ConvertFrom-Json
$config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$tenantId = ([Guid]$identity.tenant_id).ToString()
$clientId = ([Guid]$identity.client_id).ToString()
if ($tenantId -eq [Guid]::Empty.ToString() -or $clientId -eq [Guid]::Empty.ToString() -or
    $tenantId -ne $config.tenant_id -or $clientId -ne $config.client_id -or
    $config.authentication -ne 'environment') {
    throw 'Identity and environment-auth consumer configuration must specify the same tenant and dedicated application.'
}
if ($identity.certificate_thumbprint -notmatch '^[A-Fa-f0-9]{40}$') {
    throw 'A Windows CurrentUser certificate thumbprint is required.'
}
$certificate = Get-Item -LiteralPath "Cert:\CurrentUser\My\$($identity.certificate_thumbprint)"
if (-not $certificate.HasPrivateKey -or $certificate.NotAfter.ToUniversalTime() -le [DateTime]::UtcNow -or
    $certificate.NotBefore.ToUniversalTime() -gt [DateTime]::UtcNow) {
    throw 'The existing consumer certificate is not currently valid or has no private key.'
}

function ConvertTo-Base64Url([byte[]]$Bytes) {
    [Convert]::ToBase64String($Bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

$tokenEndpoint = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
$now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$header = @{
    alg = 'RS256'
    typ = 'JWT'
    x5t = ConvertTo-Base64Url $certificate.GetCertHash()
} | ConvertTo-Json -Compress
$claims = @{
    aud = $tokenEndpoint
    iss = $clientId
    sub = $clientId
    jti = [Guid]::NewGuid().ToString()
    nbf = $now - 30
    exp = $now + 300
} | ConvertTo-Json -Compress
$unsigned = (ConvertTo-Base64Url ([Text.Encoding]::UTF8.GetBytes($header))) + '.' +
    (ConvertTo-Base64Url ([Text.Encoding]::UTF8.GetBytes($claims)))
$privateKey = [Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certificate)
try {
    $signature = $privateKey.SignData(
        [Text.Encoding]::UTF8.GetBytes($unsigned),
        [Security.Cryptography.HashAlgorithmName]::SHA256,
        [Security.Cryptography.RSASignaturePadding]::Pkcs1
    )
}
finally {
    $privateKey.Dispose()
}
$assertion = $unsigned + '.' + (ConvertTo-Base64Url $signature)
try {
    $response = Invoke-RestMethod -Method Post -Uri $tokenEndpoint -MaximumRedirection 0 -Body @{
        client_id = $clientId
        scope = 'https://analysis.windows.net/powerbi/api/.default'
        grant_type = 'client_credentials'
        client_assertion_type = 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer'
        client_assertion = $assertion
    }
}
catch {
    throw 'Certificate authentication failed. Service response suppressed; no token or assertion was printed.'
}
if (-not $response.access_token) {
    throw 'Certificate authentication returned no access token.'
}

# Pass the token only to this process's child, not stdout, arguments, config, or a bundle.
$previousToken = $env:VSL_ACCESS_TOKEN
try {
    $env:VSL_ACCESS_TOKEN = $response.access_token
    $arguments = @((Join-Path $PSScriptRoot 'consumer.py'), $Operation, '--config', $ConfigPath)
    if ($Apply) { $arguments += '--apply' }
    & python @arguments
    if ($LASTEXITCODE -ne 0) { throw 'The certificate consumer failed; see its sanitized error above.' }
}
finally {
    $env:VSL_ACCESS_TOKEN = $previousToken
    $response = $null
    $assertion = $null
}
