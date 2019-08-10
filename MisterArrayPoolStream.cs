using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Marius.Mister
{
    public sealed class MisterArrayPoolStream : MemoryStream
    {
        private readonly ArrayPool<byte> _arrayPool;

        private byte[] _buffer;
        private int _length;
        private int _position;
        private bool _isDisposed;

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public override bool CanTimeout => false;

        public override long Length => _length;

        public override long Position
        {
            get { return _position; }

            set { _position = (int)value; }
        }

        public override int Capacity => _buffer?.Length ?? 0;

        public MisterArrayPoolStream()
            : this(ArrayPool<byte>.Shared)
        {
        }

        public MisterArrayPoolStream(ArrayPool<byte> arrayPool, int capacity = 0)
            : base(Array.Empty<byte>())
        {
            _arrayPool = arrayPool ?? throw new ArgumentNullException(nameof(arrayPool));
            if (capacity > 0)
                _buffer = _arrayPool.Rent(capacity);
        }

        public override byte[] GetBuffer()
        {
            CheckDisposed();

            if (_buffer == null)
                return Array.Empty<byte>();

            return _buffer;
        }

        public override bool TryGetBuffer(out ArraySegment<byte> buffer)
        {
            CheckDisposed();

            buffer = new ArraySegment<byte>(GetBuffer(), 0, _length);
            return true;
        }

        public override byte[] ToArray()
        {
            CheckDisposed();

            if (_buffer == null)
                return Array.Empty<byte>();

            var newBuffer = new byte[_length];
            Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _length);
            return newBuffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override long Seek(long offset, SeekOrigin origin)
        {
            CheckDisposed();

            switch (origin)
            {
                case SeekOrigin.Current:
                    if (_position + offset < 0 || _position + offset > Capacity)
                        throw new ArgumentOutOfRangeException(nameof(offset));

                    _position += (int)offset;
                    break;

                case SeekOrigin.Begin:
                    if (offset < 0 || offset > Capacity)
                        throw new ArgumentOutOfRangeException(nameof(offset));

                    _position = (int)offset;
                    break;

                case SeekOrigin.End:
                    if (Length + offset < 0)
                        throw new ArgumentOutOfRangeException(nameof(offset));

                    if (Length + offset > Capacity)
                        SetCapacity((int)(Length + offset));

                    _position = _length + (int)offset;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(origin));
            }

            if (_position > _length)
                _length = _position;

            return _position;
        }

        public override void SetLength(long value)
        {
            CheckDisposed();

            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value));

            if (value > Capacity)
                SetCapacity((int)value);

            _length = (int)value;
            if (_position > _length)
                _position = _length;
        }

        public override void Flush()
        {
            CheckDisposed();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (count == 0)
                return 0;

            var available = _length - _position;
            if (count > available)
                count = available;

            Buffer.BlockCopy(_buffer, _position, buffer, offset, count);
            _position += count;
            return count;
        }

        public override int ReadByte()
        {
            if (_position >= _length)
                return -1;

            return _buffer[_position++];
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled<int>(cancellationToken);

            try
            {
                var read = Read(buffer, offset, count);
                return Task.FromResult(read);
            }
            catch (Exception ex)
            {
                return Task.FromException<int>(ex);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (count == 0)
                return;

            if (Capacity - _position < count)
                SetCapacity(2 * (_position + count));

            Buffer.BlockCopy(buffer, offset, _buffer, _position, count);

            _position += count;
            if (_position > _length)
                _length = _position;
        }

        public override void WriteByte(byte value)
        {
            if (Capacity - _position < 1)
                SetCapacity(2 * (_position + 1));

            _buffer[_position++] = value;
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (cancellationToken.IsCancellationRequested)
                return Task.FromCanceled(cancellationToken);

            try
            {
                Write(buffer, offset, count);
                return Task.CompletedTask;
            }
            catch (Exception ex)
            {
                return Task.FromException(ex);
            }
        }

        public override void WriteTo(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            CheckDisposed();

            stream.Write(_buffer, 0, _length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _isDisposed = true;
                _position = 0;
                _length = 0;

                if (_buffer != null)
                {
                    _arrayPool.Return(_buffer);
                    _buffer = null;
                }
            }

            base.Dispose(disposing);
        }

        private void SetCapacity(int newCapacity)
        {
            var newData = _arrayPool.Rent(newCapacity);

            if (_buffer != null)
            {
                Buffer.BlockCopy(_buffer, 0, newData, 0, _position);
                _arrayPool.Return(_buffer);
            }

            _buffer = newData;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(MisterArrayPoolStream));
        }
    }
}
