<#
  用「干净环境」启动客户端。

  为什么要它：pydantic-settings 的优先级是「环境变量 > .env > 默认值」。如果启动这个
  进程的那个终端里设过 `CLIENT_*` 之类的变量（粘过一段设置命令、或从别的脚本继承），
  那 `.env` 里怎么改都不生效 —— 页面会提示「这个键在启动的环境变量里也有（环境变量优先，
  写文件不会生效）」。这个脚本先把**跟配置同名的**环境变量从这个会话里清掉，再启动，
  于是 `.env` 是唯一来源。

  它**不写死键名清单**：键名是从 `Settings.model_fields` 读出来的，所以以后加/删配置项
  不用改这个脚本（写死的清单迟早会漂）。

  用法：
    .\start-client.ps1                # 等价于 --ui（开页面）
    .\start-client.ps1 --once         # 跑一轮就退出（计划任务用这个）
    .\start-client.ps1 --loop         # 常驻
    .\start-client.ps1 --status       # 只看配置/源库/镜像/身份，不写任何东西
    .\start-client.ps1 --mark-unread  # 把已处理的标成未读（下一轮重抽）
#>
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
    throw "找不到 $python —— 先按 README 建虚拟环境：python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}
if (-not (Test-Path (Join-Path $PSScriptRoot '.env'))) {
    Write-Warning "这个目录下还没有 .env。第一次用可以先 .\start-client.ps1 --ui，在页面上填完保存（会写到 .env）。"
}

# 1) 清掉这个会话里跟配置同名的环境变量（清之前先列出来，不静默）
$keys = & $python -c "from app.config import Settings; print(' '.join(n.upper() for n in Settings.model_fields))"
if ($LASTEXITCODE -ne 0) { throw '读配置项清单失败（app.config 导不进来？）' }
$cleared = @()
foreach ($key in ($keys -split '\s+')) {
    if (-not $key) { continue }
    if (Test-Path "Env:$key") { $cleared += $key; Remove-Item "Env:$key" }
}
if ($cleared.Count -gt 0) {
    Write-Host ("清掉这个窗口里的环境变量 {0} 个：{1}" -f $cleared.Count, ($cleared -join ', ')) -ForegroundColor Yellow
    Write-Host '  （它们的值只在启动时被读过一次，清掉后以 .env 为准）' -ForegroundColor DarkGray
} else {
    Write-Host '这个窗口里没有跟配置同名的环境变量 —— .env 是唯一来源' -ForegroundColor Green
}

# 2) 把**生效**的几个值打出来（只读，不发任何请求）
# 2) 把**生效**的几个值打出来（只读，不发任何请求）
#    ⚠️ 给原生程序传参时 PowerShell 会啃掉里层的双引号 —— 所以 Python 那边一律用单引号，
#    这一整段用双引号包（里面没有 `$`，不会被展开）。
& $python -c "from app.config import get_settings as g; s = g(); print('生效配置：后端=%s  抽取器=%s  白名单群=%s  输入=%s' % (s.backend_base, s.client_extractor, s.client_group_whitelist or '（不限）', 'nt_msg.db 自己解密' if s.ntmsg_pipeline_enabled else '现成的导出库'))"

# 3) 启动（不给参数就当 --ui）
if (-not $Rest -or $Rest.Count -eq 0) { $Rest = @('--ui') }
& $python -m app.main @Rest
exit $LASTEXITCODE
