using System.Diagnostics;
using Fld1.Engine.Db;
using Xunit;

namespace Fld1.Engine.Tests;

public sealed class FldDatabaseProcessOwnershipTests
{
    [Fact]
    public async Task Second_process_cannot_open_database_owned_by_another_process()
    {
        const uint namespaceId = 91;
        byte[] author = Enumerable.Repeat((byte)0x91, 16).ToArray();
        byte[] recordId = FldDatabase.NewRecordId();
        string directory = Path.Combine(Path.GetTempPath(), "fld-owner-" + Guid.NewGuid().ToString("N"));
        string readyPath = Path.Combine(directory, "owner-ready");
        string ownerPidPath = Path.Combine(directory, "owner-pid");
        Directory.CreateDirectory(directory);

        string? projectDirectory = AppContext.BaseDirectory;
        while (projectDirectory is not null &&
               !File.Exists(Path.Combine(projectDirectory, "Fld1.Engine.Tests.csproj")))
            projectDirectory = Directory.GetParent(projectDirectory)?.FullName;
        Assert.NotNull(projectDirectory);

        string configuration = Directory.GetParent(AppContext.BaseDirectory.TrimEnd(
            Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar))?.Name ?? "Release";
        var start = new ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            WorkingDirectory = projectDirectory!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        start.ArgumentList.Add("test");
        start.ArgumentList.Add(Path.Combine(projectDirectory!, "Fld1.Engine.Tests.csproj"));
        start.ArgumentList.Add("--no-build");
        start.ArgumentList.Add("--no-restore");
        start.ArgumentList.Add("--configuration");
        start.ArgumentList.Add(configuration);
        start.ArgumentList.Add("--filter");
        start.ArgumentList.Add("FullyQualifiedName~Fld_database_writer_owner_child_probe");
        start.ArgumentList.Add("--results-directory");
        start.ArgumentList.Add(Path.Combine(directory, "child-results"));
        start.Environment["FLD_OWNER_TEST_DIR"] = directory;
        start.Environment["FLD_OWNER_TEST_READY"] = readyPath;
        start.Environment["FLD_OWNER_TEST_PID"] = ownerPidPath;

        Process? child = null;
        try
        {
            child = Process.Start(start);
            Assert.NotNull(child);
            Task<string> stdout = child!.StandardOutput.ReadToEndAsync();
            Task<string> stderr = child.StandardError.ReadToEndAsync();

            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!File.Exists(readyPath) && !child.HasExited && DateTime.UtcNow < deadline)
                await Task.Delay(25);

            if (!File.Exists(readyPath))
            {
                if (!child.HasExited)
                    child.Kill(entireProcessTree: true);
                await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
                Assert.True(File.Exists(readyPath),
                    $"Owner child did not acquire the FLD database. stdout={await stdout} stderr={await stderr}");
            }

            // The second process must fail before loading or replaying the same
            // physical database. It must not observe a partial/competing owner.
            Assert.Throws<IOException>(() => FldDatabase.Open(directory, namespaceId, author));

            int ownerPid = int.Parse(File.ReadAllText(ownerPidPath));
            using (Process owner = Process.GetProcessById(ownerPid))
            {
                owner.Kill(entireProcessTree: true);
                await owner.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
            if (!child.HasExited)
                child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));

            using var reopened = FldDatabase.Open(directory, namespaceId, author);
            Assert.True(reopened.TryGetBytes(recordId, 7, out byte[]? value));
            Assert.Equal("single-owner-durable"u8.ToArray(), value);
        }
        finally
        {
            if (child is not null)
            {
                try { if (!child.HasExited) child.Kill(entireProcessTree: true); } catch { }
                try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
                child.Dispose();
            }
            try { Directory.Delete(directory, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Fld_database_writer_owner_child_probe()
    {
        string? directory = Environment.GetEnvironmentVariable("FLD_OWNER_TEST_DIR");
        string? readyPath = Environment.GetEnvironmentVariable("FLD_OWNER_TEST_READY");
        string? ownerPidPath = Environment.GetEnvironmentVariable("FLD_OWNER_TEST_PID");
        if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(readyPath) ||
            string.IsNullOrWhiteSpace(ownerPidPath))
            return;

        const uint namespaceId = 91;
        byte[] author = Enumerable.Repeat((byte)0x91, 16).ToArray();
        byte[] recordId = FldDatabase.NewRecordId();
        using var db = FldDatabase.Open(directory, namespaceId, author);
        db.Put(recordId, 7, "single-owner-durable"u8, keyEpoch: 0);
        db.Commit();

        File.WriteAllText(ownerPidPath, Environment.ProcessId.ToString(
            System.Globalization.CultureInfo.InvariantCulture));
        File.WriteAllText(readyPath, "ready");
        Thread.Sleep(Timeout.Infinite);
    }
}
