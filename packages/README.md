Local NuGet feed for development.

Expected: `BinSync.Core.2.6.5.nupkg` built from https://github.com/smersh1307n2/BIN-SYNC

```bash
cd BIN-SYNC && bash scripts/vendor-ed25519-from-ztsharp.sh
dotnet pack BinSync.Net/src/BinSync.Core -c Release -o /path/to/FLD-DB/packages
```
