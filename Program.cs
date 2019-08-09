using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Marius.Mister
{
    class Program
    {
        static void Main(string[] args)
        {
            var serializer = new MisterStringSerializer();
            var connection = MisterConnection.Create(new DirectoryInfo(@"C:\Mister"), serializer, serializer);

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
                else if (parts[0] == "setall")
                {
                    var prefix = "";
                    if (parts.Length > 1)
                        prefix = parts[1];

                    sw.Restart();
                    var tasks = new Task[4000000];
                    for (var i = 0; i < tasks.Length; i++)
                    {
                        tasks[i] = connection.SetAsync($"{prefix}{i}", $"{prefix}{i}");
                    }

                    Task.WaitAll(tasks);

                    Console.WriteLine(sw.Elapsed);
                }
            }
            connection.Close();
            Console.WriteLine(sw.Elapsed);

            Console.WriteLine("Done.");
            Console.ReadLine();
        }
    }
}
