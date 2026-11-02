$ErrorActionPreference = 'Stop'

function Get-Java21 {
    $javaExe = $null
    if ($env:JAVA_HOME) {
        $javaExe = Join-Path $env:JAVA_HOME 'bin\java.exe'
        if (-not (Test-Path -LiteralPath $javaExe)) {
            throw "需要 JDK 21，但 JAVA_HOME 指向的 Java 不存在：$javaExe"
        }
    } else {
        $javaCommand = Get-Command java.exe -ErrorAction SilentlyContinue
        if (-not $javaCommand) {
            throw '未找到 Java。请安装 JDK 21，并设置 JAVA_HOME 或将 java 加入 PATH。'
        }
        $javaExe = $javaCommand.Source
    }

    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $versionOutput = (& $javaExe -version 2>&1 | Out-String)
    $ErrorActionPreference = $previousErrorAction
    $versionMatch = [regex]::Match($versionOutput, 'version "([^"]+)"')
    if (-not $versionMatch.Success) {
        throw "无法识别 Java 版本。检测命令：$javaExe"
    }

    $version = $versionMatch.Groups[1].Value
    if ($version -match '^1\.(\d+)') {
        $major = [int]$Matches[1]
    } else {
        $major = [int](($version -split '[\.-]')[0])
    }
    if ($major -ne 21) {
        throw "项目必须使用 Java 21，当前检测到 Java $major（$version）。请设置 JAVA_HOME 指向 JDK 21 后重试。"
    }

    if (-not $env:JAVA_HOME) {
        $env:JAVA_HOME = Split-Path (Split-Path $javaExe -Parent) -Parent
    }
    Write-Host "使用 Java $version：$javaExe"
    return $javaExe
}

function Get-ProjectVersion([string]$pomPath) {
    $match = [regex]::Match((Get-Content -Raw -LiteralPath $pomPath), '<version>\s*([^<\s]+)\s*</version>')
    if (-not $match.Success) {
        throw "无法从 $pomPath 读取项目版本。"
    }
    return $match.Groups[1].Value
}

function New-ZipArchiveWithUnixPermissions([string]$sourceRoot, [string]$archivePath) {
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    $sourceRoot = (Get-Item -LiteralPath $sourceRoot).FullName.TrimEnd('\')
    $packageName = Split-Path -Leaf $sourceRoot
    $archive = [System.IO.Compression.ZipFile]::Open(
        $archivePath,
        [System.IO.Compression.ZipArchiveMode]::Create
    )
    try {
        function ConvertTo-SignedInt32([long]$value) {
            if ($value -gt [int32]::MaxValue) {
                return [int]([int64]$value - 4294967296)
            }
            return [int]$value
        }

        $directoryMode = ConvertTo-SignedInt32 0x41ED0000 # drwxr-xr-x
        $fileMode = ConvertTo-SignedInt32 0x81A40000      # -rw-r--r--
        $executableMode = ConvertTo-SignedInt32 0x81ED0000 # -rwxr-xr-x

        $rootEntry = $archive.CreateEntry("$packageName/")
        $rootEntry.ExternalAttributes = $directoryMode

        Get-ChildItem -LiteralPath $sourceRoot -Recurse -Force | ForEach-Object {
            $relativePath = $_.FullName.Substring($sourceRoot.Length).TrimStart('\').Replace('\', '/')
            $entryName = "$packageName/$relativePath"
            if ($_.PSIsContainer) {
                $entry = $archive.CreateEntry("$entryName/")
                $entry.ExternalAttributes = $directoryMode
                return
            }

            $entry = [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                $archive,
                $_.FullName,
                $entryName,
                [System.IO.Compression.CompressionLevel]::Optimal
            )
            if ($_.Extension -eq '.sh') {
                $entry.ExternalAttributes = $executableMode
            } else {
                $entry.ExternalAttributes = $fileMode
            }
        }
    } finally {
        $archive.Dispose()
    }
}

$repoRoot = $PSScriptRoot
$javaExe = Get-Java21
$maven = Get-Command mvn.cmd -ErrorAction SilentlyContinue
if (-not $maven) {
    throw '未找到 Maven。请安装 Maven，并将 mvn 加入 PATH。'
}

$version = Get-ProjectVersion (Join-Path $repoRoot 'pom.xml')
$packageName = "cengyan-$version"
$distDir = Join-Path $repoRoot 'dist'
$packageRoot = Join-Path $distDir $packageName
$windowsArchivePath = Join-Path $distDir "$packageName-windows.zip"
$linuxArchivePath = Join-Path $distDir "$packageName-linux.zip"

Write-Host '开始构建 cengyan-server 和 cengyan-studio ...'
Push-Location $repoRoot
try {
    & $maven.Source '-DskipTests' '-pl' 'cengyan-server,cengyan-studio' '-am' 'clean' 'package'
    if ($LASTEXITCODE -ne 0) {
        throw "Maven 构建失败，退出码：$LASTEXITCODE"
    }
} finally {
    Pop-Location
}

if (Test-Path -LiteralPath $packageRoot) {
    Remove-Item -Recurse -Force -LiteralPath $packageRoot
}
foreach ($archivePath in @($windowsArchivePath, $linuxArchivePath)) {
    if (Test-Path -LiteralPath $archivePath) {
        Remove-Item -Force -LiteralPath $archivePath
    }
}

$null = New-Item -ItemType Directory -Force -Path `
    (Join-Path $packageRoot 'bin'),
    (Join-Path $packageRoot 'config'),
    (Join-Path $packageRoot 'data'),
    (Join-Path $packageRoot 'logs'),
    (Join-Path $packageRoot 'pid'),
    (Join-Path $packageRoot 'tmp'),
    (Join-Path $packageRoot 'lib')

$serverJar = Join-Path $repoRoot "cengyan-server\target\cengyan-server-$version.jar"
$studioJar = Join-Path $repoRoot "cengyan-studio\target\cengyan-studio-$version.jar"
foreach ($jar in @($serverJar, $studioJar)) {
    if (-not (Test-Path -LiteralPath $jar)) {
        throw "未找到构建产物：$jar"
    }
}
Copy-Item -LiteralPath $serverJar -Destination (Join-Path $packageRoot 'cengyan-server.jar')
Copy-Item -LiteralPath $studioJar -Destination (Join-Path $packageRoot 'cengyan-studio.jar')

Copy-Item -LiteralPath (Join-Path $repoRoot 'cengyan.properties') -Destination (Join-Path $packageRoot 'config\cengyan.properties')
Copy-Item -LiteralPath (Join-Path $repoRoot 'cengyan.properties') -Destination (Join-Path $packageRoot 'config\cengyan.properties.example')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\studio.properties') -Destination (Join-Path $packageRoot 'config\studio.properties')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\studio.properties') -Destination (Join-Path $packageRoot 'config\studio.properties.example')

Copy-Item -Path (Join-Path $repoRoot 'packaging\bin\*') -Destination (Join-Path $packageRoot 'bin') -Recurse
$utf8 = New-Object System.Text.UTF8Encoding $false
Get-ChildItem -LiteralPath (Join-Path $packageRoot 'bin') -File | Where-Object { $_.Extension -eq '.sh' -or $_.Name -eq 'java.env' } | ForEach-Object {
    $text = [System.IO.File]::ReadAllText($_.FullName) -replace "`r`n", "`n" -replace "`r", "`n"
    [System.IO.File]::WriteAllText($_.FullName, $text, $utf8)
}
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\PACKAGE-README.txt') -Destination (Join-Path $packageRoot 'README.txt')
Copy-Item -LiteralPath (Join-Path $repoRoot 'packaging\lib\README.txt') -Destination (Join-Path $packageRoot 'lib\README.txt')

for ($attempt = 1; $attempt -le 3; $attempt++) {
    try {
        Compress-Archive -Path $packageRoot -DestinationPath $windowsArchivePath -CompressionLevel Optimal -ErrorAction Stop
        break
    } catch {
        if ($attempt -eq 3) {
            throw
        }
        Start-Sleep -Seconds 2
    }
}

$linuxTempRoot = Join-Path ([System.IO.Path]::GetTempPath()) "$packageName-linux-$PID"
$linuxPackageRoot = Join-Path $linuxTempRoot $packageName
try {
    $null = New-Item -ItemType Directory -Force -Path `
        (Join-Path $linuxPackageRoot 'bin'),
        (Join-Path $linuxPackageRoot 'config'),
        (Join-Path $linuxPackageRoot 'data'),
        (Join-Path $linuxPackageRoot 'logs'),
        (Join-Path $linuxPackageRoot 'pid'),
        (Join-Path $linuxPackageRoot 'tmp'),
        (Join-Path $linuxPackageRoot 'lib')
    Copy-Item -LiteralPath (Join-Path $packageRoot 'cengyan-server.jar') -Destination $linuxPackageRoot
    Copy-Item -LiteralPath (Join-Path $packageRoot 'cengyan-studio.jar') -Destination $linuxPackageRoot
    Copy-Item -LiteralPath (Join-Path $packageRoot 'README.txt') -Destination $linuxPackageRoot
    Copy-Item -Path (Join-Path $packageRoot 'config\*') -Destination (Join-Path $linuxPackageRoot 'config') -Recurse
    Copy-Item -Path (Join-Path $packageRoot 'lib\*') -Destination (Join-Path $linuxPackageRoot 'lib') -Recurse
    Copy-Item -Path (Join-Path $packageRoot 'bin\*.sh') -Destination (Join-Path $linuxPackageRoot 'bin')
    Copy-Item -LiteralPath (Join-Path $packageRoot 'bin\java.env') -Destination (Join-Path $linuxPackageRoot 'bin')
    New-ZipArchiveWithUnixPermissions $linuxPackageRoot $linuxArchivePath
} finally {
    if (Test-Path -LiteralPath $linuxTempRoot) {
        Remove-Item -Recurse -Force -LiteralPath $linuxTempRoot
    }
}
Write-Host "打包完成：$packageRoot"
Write-Host "Windows 压缩包：$windowsArchivePath"
Write-Host "Linux 压缩包：$linuxArchivePath（已保留 .sh 的可执行权限）"


