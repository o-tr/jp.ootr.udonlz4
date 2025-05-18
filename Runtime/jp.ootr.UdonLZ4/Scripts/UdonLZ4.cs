using System;
using jp.ootr.common;
using UdonSharp;
using UnityEngine;

namespace jp.ootr.UdonLZ4
{
    [UdonBehaviourSyncMode(BehaviourSyncMode.NoVariableSync)]
    public class UdonLZ4 : UdonSharpBehaviour
    {
        private const int ASYNC_DELAY = 1;
        
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

        private UdonSharpBehaviour[] _lz4CallbackReceivers = new UdonSharpBehaviour[0];
        private long _lz4DIndex;

        private byte[] _lz4Dist = new byte[0];
        private bool _lz4HasBlockSum;
        private bool _lz4HasContentSum;
        private long[] _lz4MaxSizes = new long[0];
        private long _lz4SEnd;
        private long _lz4SIndex;
        private long _lz4SLength;
        private float _lz4StartTime;
        
        private bool _lz4IsAsync;
        
        private byte[] _lz4DecompressedData = new byte[0];

        public void DecompressAsync(UdonSharpBehaviour self, byte[] src, long maxSize = 0)
        {
            _lz4CallbackReceivers = _lz4CallbackReceivers.Append((LZ4CallbackReceiver)self);
            _lz4Buffer = _lz4Buffer.Append(src);
            _lz4MaxSizes = _lz4MaxSizes.Append(maxSize);
            if (_lz4IsAsync)
            {
                Debug.Log("[UdonLZ4] DecompressAsync: adding to queue");
                return;
            }
            Debug.Log($"[UdonLZ4] DecompressAsync: start processing {src.Length} bytes");
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), 10);
        }

        public void __DecompressItemAsync()
        {
            if (_lz4CallbackReceivers.Length == 0)
            {
                _lz4IsAsync = false;
                return;
            }
            _lz4IsAsync = true;
            _lz4StartTime = Time.realtimeSinceStartup;

            if (!ValidateData(_lz4Buffer[0], out var contentIndex, out var hasBlockSum, out var hasContentSum,
                    out var hasContentSize, out var hasDictId, out var maxBlockSize, out var error, out var maxContentSize))
            {
                OnDecompressError(error);
                return;
            }

            if (hasContentSize)
            {
                _lz4MaxSizes[0] = maxContentSize;
            }

            // skip checksum
            contentIndex++;

            if (_lz4MaxSizes[0] == 0)
            {
                _lz4MaxSizes[0] = DecompressBound(_lz4Buffer[0], contentIndex, maxBlockSize, hasBlockSum);
                if (_lz4MaxSizes[0] == 0)
                {
                    OnDecompressError(DecompressError.InvalidBlockSize);
                    return;
                }
            }

            _lz4Dist = new byte[_lz4MaxSizes[0]];
            _lz4HasBlockSum = hasBlockSum;
            _lz4HasContentSum = hasContentSum;
            _lz4SIndex = contentIndex;
            _lz4DIndex = 0;

            __DecompressFrameInternalAsync();
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
                if (_lz4DIndex == _lz4Dist.Length)
                {
                    OnDecompressSuccess(_lz4Dist);
                    return;
                }

                var tmpArray = new byte[_lz4DIndex];
                Array.Copy(_lz4Dist, 0, tmpArray, 0, _lz4DIndex);
                OnDecompressSuccess(tmpArray);
                return;
            }

            if (_lz4HasBlockSum)
            {
                _lz4SIndex += 4;
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
                _DecompressBlockInternalAsync();
            }
        }

        public void _DecompressBlockInternalAsync()
        {
            _lz4StartTime = Time.realtimeSinceStartup;
            
            __DecompressBlockInternalAsync();
        }

        private void __DecompressBlockInternalAsync()
        {
            while (_lz4SIndex < _lz4SEnd)
            {
                if (DecompressBlockInternal(_lz4Buffer[0], _lz4Dist, ref _lz4SIndex, ref _lz4DIndex)) break;

                if (!(Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)) continue;
                SendCustomEventDelayedFrames(nameof(_DecompressBlockInternalAsync), ASYNC_DELAY);
                return;
            }

            __DecompressFrameInternalAsyncEnd();
        }

        private void __DecompressFrameInternalAsyncEnd()
        {
            if (_lz4HasContentSum)
                _lz4SIndex += 4;
            
            if (Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)
            {
                SendCustomEventDelayedFrames(nameof(_DecompressFrameInternalAsync), ASYNC_DELAY);
                return;
            }

            __DecompressFrameInternalAsync();
        }
        
        private void OnDecompressSuccess(byte[] result)
        {
            _lz4CallbackReceivers = _lz4CallbackReceivers.Shift(out var device);
            _lz4DecompressedData = result;
            _lz4Dist = null;
            _lz4Buffer = _lz4Buffer.Shift();
            _lz4MaxSizes = _lz4MaxSizes.Shift();
            if (device != null) device.SendCustomEventDelayedFrames("OnLZ4Decompress", ASYNC_DELAY);
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), ASYNC_DELAY);
        }

        private void OnDecompressError(DecompressError error)
        {
            Debug.LogError($"[UdonLZ4] Failed to decompress: {error}");
            _lz4CallbackReceivers = _lz4CallbackReceivers.Shift(out var device);
            if (device != null) device.SendCustomEvent("OnLZ4DecompressError");
            _lz4DecompressedData = null;
            _lz4Dist = null;
            _lz4Buffer = _lz4Buffer.Shift();
            _lz4MaxSizes = _lz4MaxSizes.Shift();
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), ASYNC_DELAY);
        }

        private bool ValidateData(byte[] src, out long contentIndex, out bool hasBlockSum, out bool hasContentSum,
            out bool hasContentSize, out bool hasDictId, out long maxBlockSize, out DecompressError error, out long maxContentSize)
        {
            contentIndex = -1;
            hasBlockSum = false;
            hasContentSum = false;
            hasContentSize = false;
            hasDictId = false;
            maxBlockSize = 0;
            maxContentSize = 0;
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

            if (hasContentSize)
            {
                maxContentSize = (long)ReadU64(src, ref sIndex);
                return true;
            }


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
            return 0;
        }

        private bool DecompressBlockInternal(byte[] src, byte[] dst, ref long sIndex, ref long dIndex)
        {
            var token = src[sIndex++];

            long literalCount = token >> 4;
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

                Array.Copy(src, sIndex, dst, dIndex, literalCount);
                sIndex += literalCount;
                dIndex += literalCount;
            }

            if (sIndex >= _lz4SEnd)
            {
                return true;
            }

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
                // Manually fill the region [dIndex, dIndex + mLength) with the byte value dst[dIndex - 1]
                byte fillValue = dst[dIndex - 1];
                dst[dIndex] = fillValue;
                int filled = 1;
                while (filled < mLength)
                {
                    int copyLength = (int)Math.Min(filled, mLength - filled);
                    Array.Copy(dst, dIndex, dst, dIndex + filled, copyLength);
                    filled += copyLength;
                }
            } 
            else if (mOffset < mLength)
            {
                Array.Copy(dst, dIndex - mOffset, dst, dIndex, mOffset);
                long copied = mOffset;
                while (copied < mLength)
                {
                    int chunk = (int)Math.Min(copied, mLength - copied);
                    Array.Copy(dst, dIndex, dst, dIndex + copied, chunk);
                    copied += chunk;
                }
            } 
            else
            {
                Array.Copy(dst, dIndex - mOffset, dst, dIndex, mLength);
            }

            dIndex += mLength;

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
        
        private static ulong ReadU64(byte[] b, ref long n)
        {
            ulong x = 0;
            x |= (ulong)b[n++] << 0;
            x |= (ulong)b[n++] << 8;
            x |= (ulong)b[n++] << 16;
            x |= (ulong)b[n++] << 24;
            x |= (ulong)b[n++] << 32;
            x |= (ulong)b[n++] << 40;
            x |= (ulong)b[n++] << 48;
            x |= (ulong)b[n++] << 56;
            return x;
        }
        
        public byte[] GetDecompressedData()
        {
            return _lz4DecompressedData;
        }

        public void ClearDecompressedData()
        {
            _lz4DecompressedData = null;
        }
    }
}
