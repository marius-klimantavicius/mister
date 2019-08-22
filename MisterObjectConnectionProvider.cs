using System.IO;
using FASTER.core;

namespace Marius.Mister
{
    public struct MisterObjectConnectionProvider<TKey, TValue, TKeyAtomSource, TValueAtomSource> : IMisterConnectionProvider<TKey, TValue, MisterObject, TKeyAtomSource, MisterObject, TValueAtomSource>
        where TKeyAtomSource : struct, IMisterAtomSource<MisterObject>
        where TValueAtomSource : struct, IMisterAtomSource<MisterObject>
    {
        public IMisterSerializer<TKey, MisterObject, TKeyAtomSource> KeySerializer { get; }

        public IMisterSerializer<TValue, MisterObject, TValueAtomSource> ValueSerializer { get; }

        public MisterObjectConnectionProvider(IMisterSerializer<TKey, MisterObject, TKeyAtomSource> keySerializer, IMisterSerializer<TValue, MisterObject, TValueAtomSource> valueSerializer)
        {
            KeySerializer = keySerializer;
            ValueSerializer = valueSerializer;
        }

        public IDevice CreateDevice(DirectoryInfo directory)
        {
            return Devices.CreateLogDevice(Path.Combine(directory.FullName, @"hlog.log"));
        }

        public IFasterKV<MisterObject, MisterObject, byte[], TValue, object> Create(DirectoryInfo directory, MisterConnectionSettings settings, IDevice mainDevice)
        {
            var environment = new MisterObjectEnvironment<TValue, TValueAtomSource>(ValueSerializer);
            var variableLengthStructSettings = new VariableLengthStructSettings<MisterObject, MisterObject>()
            {
                keyLength = new MisterObjectVariableLengthStruct(),
                valueLength = new MisterObjectVariableLengthStruct(),
            };

            mainDevice = Devices.CreateLogDevice(Path.Combine(directory.FullName, @"hlog.log"));
            var logSettings = new LogSettings
            {
                LogDevice = mainDevice,
            };
            settings.Apply(logSettings);

            return new FasterKV<MisterObject, MisterObject, byte[], TValue, object, MisterObjectEnvironment<TValue, TValueAtomSource>>(
                settings.IndexSize,
                environment,
                logSettings,
                new CheckpointSettings() { CheckpointDir = directory.FullName, CheckPointType = CheckpointType.FoldOver },
                serializerSettings: null,
                comparer: MisterObjectEqualityComparer.Instance,
                variableLengthStructSettings: variableLengthStructSettings
            );
        }
    }
}
