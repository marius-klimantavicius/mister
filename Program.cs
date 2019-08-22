using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Marius.Mister
{
    class Program
    {
        static void Main(string[] args)
        {
            var serializer = new MisterObjectStringSerializer();
            var settings = new MisterConnectionSettings()
            {
                PageSizeBits = 20,
                SegmentSizeBits = 21,
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
                    connection.SetAsync(parts[1], parts[2]).GetAwaiter().GetResult();
                }
                else if (parts[0] == "get" && parts.Length > 1)
                {
                    var value = connection.GetAsync(parts[1]).GetAwaiter().GetResult();
                    Console.WriteLine(value);
                }
                else if (parts[0] == "cp")
                {
                    connection.Checkpoint();
                }
                else if (parts[0] == "flush")
                {
                    connection.FlushAsync(true).GetAwaiter().GetResult();
                }
                else if (parts[0] == "del" && parts.Length > 1)
                {
                    connection.DeleteAsync(parts[1]).GetAwaiter().GetResult();
                }
                else if (parts[0] == "en")
                {
                    var semaphore = new SemaphoreSlim(0);
                    connection.ForEach((key, value, isDeleted, _) =>
                    {
                        Console.WriteLine($"{(isDeleted ? "[dead] " : "")}{key} - {value}");
                    },
                    (sem) => ((SemaphoreSlim)sem).Release(), semaphore);
                    semaphore.Wait();
                }
                else if (parts[0] == "setn" && parts.Length > 1)
                {
                    if (!int.TryParse(parts[1], out var count))
                        continue;

                    var prefix = "";
                    if (parts.Length > 2)
                        prefix = parts[2];

                    sw.Restart();
                    var tasks = new Task[count];
                    for (var i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = connection.SetAsync($"{prefix}{i}", $"{prefix}{i}");
                    }

                    Task.WaitAll(tasks);
                    Console.WriteLine(sw.Elapsed);
                }
                else if (parts[0] == "compact")
                {
                    connection.CompactAsync().GetAwaiter().GetResult();
                }
            }
            connection.Close();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done.");
            Console.ReadLine();
        }
    }
}
