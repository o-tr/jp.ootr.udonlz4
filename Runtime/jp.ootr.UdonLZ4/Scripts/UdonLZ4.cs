using System;
using UdonSharp;
using UnityEngine;

namespace jp.ootr.UdonLZ4
{
    [UdonBehaviourSyncMode(BehaviourSyncMode.None)]
    public class UdonLZ4 : UdonSharpBehaviour
    {
        private const long MIN_MATCH = 4;

        private const int FD_BLOCK_CHKSUM = 0b0001_0000;
        private const int FD_CONTENT_SIZE = 0b0000_1000;
        private const int FD_CONTENT_CHKSUM = 0b0000_0100;
        private const int FD_DICT_ID = 0b0000_0001;
        private const int MAGIC_NUM = 0x184D2204;
        private const int FD_VERSION = 0x40;
        private const int FD_VERSION_MASK = 0b1100_0000;

        private const uint BS_UNCOMPRESSED = 0x80000000;
        private const int BS_SHIFT = 4;
        private const int BS_MASK = 7;

        private const float MaxFrameTime = 0.005f;
        private readonly long[] _bsMap = { 0, 0, 0, 0, 0x10000, 0x40000, 0x100000, 0x400000 };
        private byte[][] _lz4Buffer = new byte[0][];

        private LZ4CallbackReceiver[] _lz4CallbackReceivers = new LZ4CallbackReceiver[0];
        private long _lz4DIndex;

        private byte[] _lz4Dist = new byte[0];
        private bool _lz4HasBlockSum;
        private long[] _lz4MaxSizes = new long[0];
        private long _lz4SEnd;
        private long _lz4SIndex;
        private long _lz4SLength;
        private float _lz4StartTime;

        public byte[] Decompress(byte[] src, long maxSize = -1)
        {
            if (!ValidateData(src, out var contentIndex, out var hasBlockSum, out var hasContentSum,
                    out var hasContentSize, out var hasDictId, out var maxBlockSize, out var error))
            {
                Debug.LogError("invalid input data");
                return new byte[0];
            }

            if (maxSize < 0)
            {
                maxSize = DecompressBound(src, contentIndex, maxBlockSize, hasBlockSum);
                if (maxSize < 0) return new byte[0];
            }

            var dist = new byte[maxSize];
            var size = DecompressFrame(src, dist, contentIndex, hasBlockSum);

            if (size == maxSize) return dist;
            var tmpArray = new byte[size];
            Array.Copy(dist, 0, tmpArray, 0, size);
            return tmpArray;
        }

        public void DecompressAsync(ILZ4CallbackReceiver self, byte[] src, long maxSize = -1)
        {
            _lz4CallbackReceivers = _lz4CallbackReceivers.Append((LZ4CallbackReceiver)self);
            _lz4Buffer = _lz4Buffer.Append(src);
            _lz4MaxSizes = _lz4MaxSizes.Append(maxSize);

            if (_lz4CallbackReceivers.Length > 1) return;
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), 1);
        }

        public void __DecompressItemAsync()
        {
            if (_lz4CallbackReceivers.Length == 0) return;
            if (!ValidateData(_lz4Buffer[0], out var contentIndex, out var hasBlockSum, out var hasContentSum,
                    out var hasContentSize, out var hasDictId, out var maxBlockSize, out var error))
            {
                OnDecompressError(error);
                return;
            }

            if (_lz4MaxSizes[0] < 0)
            {
                _lz4MaxSizes[0] = DecompressBound(_lz4Buffer[0], contentIndex, maxBlockSize, hasBlockSum);
                if (_lz4MaxSizes[0] < 0)
                {
                    OnDecompressError(DecompressError.InvalidBlockSize);
                    return;
                }
            }

            _lz4Dist = new byte[_lz4MaxSizes[0]];
            _lz4HasBlockSum = hasBlockSum;
            _lz4SIndex = contentIndex;
            _lz4DIndex = 0;

            _DecompressFrameInternalAsync();
        }

        public void _DecompressFrameInternalAsync()
        {
            _lz4StartTime = Time.realtimeSinceStartup;
            __DecompressFrameInternalAsync();
        }

        private void __DecompressFrameInternalAsync()
        {
            var compSize = ReadU32(_lz4Buffer[0], ref _lz4SIndex);
            if (compSize == 0)
            {
                Debug.Log($"export: {_lz4DIndex} {_lz4Dist.Length}");
                if (_lz4DIndex == _lz4Dist.Length)
                {
                    var device = _lz4CallbackReceivers[0];
                    device.OnLZ4Decompress(_lz4Dist);
                    return;
                }

                var tmpArray = new byte[_lz4DIndex];
                Array.Copy(_lz4Dist, 0, tmpArray, 0, _lz4DIndex);
                _lz4CallbackReceivers[0].OnLZ4Decompress(_lz4Dist);
                return;
            }

            if ((compSize & BS_UNCOMPRESSED) != 0)
            {
                compSize &= ~BS_UNCOMPRESSED;

                Array.Copy(_lz4Buffer[0], _lz4SIndex, _lz4Dist, _lz4DIndex, compSize);
                _lz4SIndex += compSize;
                _lz4DIndex += compSize;
                __DecompressFrameInternalAsyncEnd();
            }
            else
            {
                _lz4SLength = compSize;
                _lz4SEnd = _lz4SIndex + _lz4SLength;
                __DecompressBlockInternalAsync();
            }
        }

        public void _DecompressBlockInternalAsync()
        {
            _lz4StartTime = Time.realtimeSinceStartup;
            __DecompressBlockInternalAsync();
        }

        public void __DecompressBlockInternalAsync()
        {
            while (_lz4SIndex < _lz4SEnd)
            {
                if (DecompressBlockInternal(_lz4Buffer[0], _lz4Dist, ref _lz4SIndex, ref _lz4DIndex, _lz4SEnd)) break;

                if (Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)
                {
                    SendCustomEventDelayedFrames(nameof(_DecompressBlockInternalAsync), 1);
                    return;
                }
            }

            __DecompressFrameInternalAsyncEnd();
        }

        private void __DecompressFrameInternalAsyncEnd()
        {
            if (_lz4HasBlockSum)
                // TODO: read block checksum
                _lz4SIndex += 4;

            if (Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)
            {
                SendCustomEventDelayedFrames(nameof(_DecompressFrameInternalAsync), 1);
                return;
            }

            __DecompressFrameInternalAsync();
        }

        private void OnDecompressError(DecompressError error)
        {
            _lz4CallbackReceivers[0].OnLZ4DecompressError(error);
            _lz4CallbackReceivers = _lz4CallbackReceivers.Remove(0);
            _lz4Buffer = _lz4Buffer.Remove(0);
            _lz4MaxSizes = _lz4MaxSizes.Remove(0);
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), 1);
        }

        private bool ValidateData(byte[] src, out long contentIndex, out bool hasBlockSum, out bool hasContentSum,
            out bool hasContentSize, out bool hasDictId, out long maxBlockSize, out DecompressError error)
        {
            contentIndex = -1;
            hasBlockSum = false;
            hasContentSum = false;
            hasContentSize = false;
            hasDictId = false;
            maxBlockSize = 0;
            error = DecompressError.None;
            long sIndex = 0;

            if (ReadU32(src, ref sIndex) != MAGIC_NUM)
            {
                Debug.LogWarning("invalid magic number");
                error = DecompressError.InvalidMagicNumber;
                return false;
            }

            var descriptor = src[sIndex++];

            if ((descriptor & FD_VERSION_MASK) != FD_VERSION)
            {
                Debug.LogWarning("invalid version");
                error = DecompressError.InvalidVersion;
                return false;
            }

            hasBlockSum = (descriptor & FD_BLOCK_CHKSUM) != 0;
            hasContentSum = (descriptor & FD_CONTENT_CHKSUM) != 0;
            hasContentSize = (descriptor & FD_CONTENT_SIZE) != 0;
            hasDictId = (descriptor & FD_DICT_ID) != 0;

            // Read block size
            var bsIdx = (src[sIndex++] >> BS_SHIFT) & BS_MASK;
            maxBlockSize = _bsMap[bsIdx];
            if (maxBlockSize == 0)
            {
                Debug.LogWarning("invalid block size");
                error = DecompressError.InvalidBlockSize;
                return false;
            }

            if (hasContentSize) sIndex += 8;

            if (hasDictId) sIndex += 4;
            // Header Checksum
            sIndex++;

            contentIndex = sIndex;
            return true;
        }

        private long DecompressBound(byte[] src, long sIndex, long maxBlockSize, bool hasBlockSum)
        {
            long maxSize = 0;
            while (sIndex < src.Length)
            {
                var blockSize = ReadU32(src, ref sIndex);

                if ((blockSize & BS_UNCOMPRESSED) != 0)
                {
                    blockSize &= ~BS_UNCOMPRESSED;
                    maxSize += blockSize;
                }
                else if (blockSize > 0)
                {
                    maxSize += maxBlockSize;
                }

                if (blockSize == 0) return maxSize;

                if (hasBlockSum)
                    // ignore block checksum
                    sIndex += 4;

                sIndex += blockSize;
            }

            Debug.LogWarning("invalid block size");
            return -1;
        }

        private long DecompressFrame(byte[] src, byte[] dist, long sIndex, bool useBlockSum)
        {
            long dIndex = 0;
            // Read blocks.
            while (true)
            {
                var compSize = ReadU32(src, ref sIndex);
                if (compSize == 0) break;
                if (useBlockSum)
                    // TODO: read block checksum
                    sIndex += 4;

                if ((compSize & BS_UNCOMPRESSED) != 0)
                {
                    compSize &= ~BS_UNCOMPRESSED;

                    for (var i = 0; i < compSize; i++) dist[dIndex++] = src[sIndex++];
                }
                else
                {
                    dIndex = DecompressBlock(src, dist, sIndex, compSize, dIndex);
                    sIndex += compSize;
                }
            }

            return dIndex;
        }

        private long DecompressBlock(byte[] src, byte[] dst, long sIndex, long sLength, long dIndex)
        {
            var sEnd = sIndex + sLength;
            while (sIndex < sEnd)
                if (DecompressBlockInternal(src, dst, ref sIndex, ref dIndex, sEnd))
                    break;

            return dIndex;
        }

        private bool DecompressBlockInternal(byte[] src, byte[] dst, ref long sIndex, ref long dIndex, long sEnd)
        {
            var token = src[sIndex++];

            var literalCount = token >> 4;
            if (literalCount > 0)
            {
                if (literalCount == 0xf)
                    while (true)
                    {
                        var lenByte = src[sIndex++];
                        literalCount += lenByte;
                        if (lenByte != 0xff)
                            break;
                    }

                for (var i = 0; i < literalCount; i++) dst[dIndex++] = src[sIndex++];
            }

            if (sIndex >= sEnd)
                return true;

            long mLength = token & 0xf;

            long mOffset = src[sIndex++] | (src[sIndex++] << 8);

            if (mLength == 0xf)
                while (true)
                {
                    var lenByte = src[sIndex++];
                    mLength += lenByte;
                    if (lenByte != 0xff)
                        break;
                }

            mLength += MIN_MATCH;

            if (mOffset == 1)
            {
                for (var j = dIndex; j < dIndex + mLength; j++) dst[j] = (byte)(dst[dIndex - 1] | 0);
                dIndex += mLength;
            }
            else
            {
                for (long i = dIndex - mOffset, n = i + mLength; i < n;) dst[dIndex++] = (byte)(dst[i++] | 0);
            }

            return false;
        }


        private static uint ReadU32(byte[] b, ref long n)
        {
            uint x = 0;
            x |= (uint)b[n++] << 0;
            x |= (uint)b[n++] << 8;
            x |= (uint)b[n++] << 16;
            x |= (uint)b[n++] << 24;
            return x;
        }
    }
}
