﻿using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public static class Program
    {
        private struct DurationLogger : IDisposable
        {
            private Stopwatch _stopwatch;

            public DurationLogger(Stopwatch stopwatch)
            {
                _stopwatch = stopwatch;
                _stopwatch.Restart();
            }

            public void Dispose()
            {
                if (_stopwatch != null)
                {
                    Console.WriteLine(_stopwatch.Elapsed);
                }
            }
        }

        public static async Task Main(string[] args)
        {
            var serializer = new MisterObjectStringUtf8Serializer();
            var settings = new MisterConnectionSettings()
            {
                PageSizeBits = 20,
                SegmentSizeBits = 25,
            };

            var connection = MisterConnection.Create(new DirectoryInfo(@"C:\Mister"), serializer, serializer, settings: settings);
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var command = Console.ReadLine();
                var parts = command.Split();
                if (parts.Length == 0)
                    continue;

                if (parts[0] == "quit")
                    break;

                if (parts[0] == "set" && parts.Length > 2)
                {
                    using (new DurationLogger(sw))
                    {
                        await connection.SetAsync(parts[1], parts[2]);
                    }
                }
                else if (parts[0] == "get" && parts.Length > 1)
                {
                    var value = await connection.GetAsync(parts[1]);
                    Console.WriteLine(value);
                }
                else if (parts[0] == "cp")
                {
                    using (new DurationLogger(sw))
                    {
                        await connection.CheckpointAsync();
                    }
                }
                else if (parts[0] == "flush")
                {
                    using (new DurationLogger(sw))
                    {
                        connection.Flush(true);
                    }
                }
                else if (parts[0] == "del" && parts.Length > 1)
                {
                    using (new DurationLogger(sw))
                    {
                        await connection.DeleteAsync(parts[1]);
                    }
                }
                else if (parts[0] == "en")
                {
                    var isSilent = false;
                    if (parts.Length > 1 && parts[1] == "silent")
                        isSilent = true;

                    using (new DurationLogger(sw))
                    {
                        using (var semaphore = new SemaphoreSlim(0))
                        {
                            connection.ForEach((key, value, isDeleted, _) =>
                            {
                                if (!isSilent)
                                    Console.WriteLine($"{(isDeleted ? "[dead] " : "")}{key} - {value}");
                            },
                            (sem) => ((SemaphoreSlim)sem).Release(), semaphore);
                            await semaphore.WaitAsync();
                        }
                    }
                }
                else if (parts[0] == "setn" && parts.Length > 1)
                {
                    if (!int.TryParse(parts[1], out var count))
                        continue;

                    var prefix = "";
                    if (parts.Length > 2)
                        prefix = parts[2];

                    using (new DurationLogger(sw))
                    {
                        var tasks = new Task[Environment.ProcessorCount * 20];
                        var rem = 0;
                        for (var i = 0; i < tasks.Length; i++)
                        {
                            tasks[i] = Task.Run(async () =>
                            {
                                await Task.Yield();

                                var c = 0;
                                do
                                {
                                    c = Interlocked.Increment(ref rem);
                                    await connection.SetAsync($"{prefix}{c}", $"{prefix}{c}");
                                }
                                while (c < count);
                            });
                        }

                        await Task.WhenAll(tasks);
                    }
                }
                else if (parts[0] == "setns" && parts.Length > 1)
                {
                    if (!int.TryParse(parts[1], out var count))
                        continue;

                    var prefix = "";
                    if (parts.Length > 2)
                        prefix = parts[2];

                    using (new DurationLogger(sw))
                    {
                        for (var i = 0; i < count; i++)
                            await connection.SetAsync($"{prefix}{i}", $"{prefix}{i}");
                    }
                }
                else if (parts[0] == "getn" && parts.Length > 1)
                {
                    if (!int.TryParse(parts[1], out var count))
                        continue;

                    var prefix = "";
                    if (parts.Length > 2)
                        prefix = parts[2];

                    using (new DurationLogger(sw))
                    {
                        var tasks = new Task[Environment.ProcessorCount * 20];
                        var rem = 0;
                        for (var i = 0; i < tasks.Length; i++)
                        {
                            tasks[i] = Task.Run(async () =>
                            {
                                await Task.Yield();

                                var c = 0;
                                do
                                {
                                    c = Interlocked.Increment(ref rem);
                                    await connection.GetAsync($"{prefix}{c}");
                                }
                                while (c < count);
                            });
                        }

                        await Task.WhenAll(tasks);
                    }
                }
                else if (parts[0] == "getns" && parts.Length > 1)
                {
                    if (!int.TryParse(parts[1], out var count))
                        continue;

                    var prefix = "";
                    if (parts.Length > 2)
                        prefix = parts[2];

                    using (new DurationLogger(sw))
                    {
                        for (var i = 0; i < count; i++)
                            await connection.GetAsync($"{prefix}{i}");
                    }
                }
            }
            connection.Close();

            Console.WriteLine("Done.");
            Console.ReadLine();
        }
    }
}