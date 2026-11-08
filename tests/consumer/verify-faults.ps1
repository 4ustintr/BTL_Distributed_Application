# Run after the full isolated suite has seeded its databases. Never targets the demo stack.
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Resolve-Path (Join-Path $PSScriptRoot '../..'))
$composeFile = 'tests/consumer/compose.integration.yml'
function Invoke-TestDocker {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker failed: $DockerArgs" }
}
# Match the two-machine layout: the host stays alive when its PG container dies.
# Docker's per-container DNS disappears on stop, which would test DNS timeout instead.
Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'exec', '-T', 'chung-hq',
    'psql', '-U', 'postgres', '-d', 'hq_db', '-v', 'ON_ERROR_STOP=1', '-c',
    "ALTER SERVER srv_dc2 OPTIONS (SET host 'host.docker.internal', SET port '15434')")
try {
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'kill', '-s', 'SIGKILL', 'chung-dc2')
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'run', '--rm', '--use-aliases', '-e', 'DC_ID=2', 'chung-runner',
        'python', '-m', 'pytest', 'tests/hq_api/test_tc12_external_faults.py::test_tc12_physically_killed_pg', '-q', '-s')
} finally {
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'up', '-d', '--wait', 'chung-dc2')
}
try {
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'pause', 'chung-dc2')
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'run', '--rm', '--use-aliases', '-e', 'DC_ID=2', 'chung-runner',
        'python', '-m', 'pytest', 'tests/hq_api/test_tc12_external_faults.py::test_tc13_physically_paused_pg', '-q', '-s')
} finally {
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'unpause', 'chung-dc2')
    Invoke-TestDocker -DockerArgs @('compose', '-f', $composeFile, 'exec', '-T', 'chung-hq',
        'psql', '-U', 'postgres', '-d', 'hq_db', '-v', 'ON_ERROR_STOP=1', '-c',
        "ALTER SERVER srv_dc2 OPTIONS (SET host 'chung-dc2', SET port '5432')")
}

