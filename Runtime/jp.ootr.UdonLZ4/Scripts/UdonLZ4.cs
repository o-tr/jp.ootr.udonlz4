using System;
using jp.ootr.common;
using UdonSharp;
using UnityEngine;

namespace jp.ootr.UdonLZ4
{
    [UdonBehaviourSyncMode(BehaviourSyncMode.Manual)]
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

        // Hard cap on output buffer to defend against malicious / corrupted headers
        // declaring an absurd content size. 256 MiB is large enough for the EIA image
        // payloads this package is used for.
        private const long MAX_OUTPUT_SIZE = 256L * 1024 * 1024;

        // Block-decode result codes
        private const int BLOCK_CONTINUE = 0;
        private const int BLOCK_END = 1;
        private const int BLOCK_ERROR = -1;

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
        private DecompressError _lz4LastError = DecompressError.None;

        public void DecompressAsync(UdonSharpBehaviour self, byte[] src, long maxSize = 0)
        {
            // null is normalized to an empty array so empty/invalid inputs flow
            // through the queue in FIFO order and surface as a normal error item
            // in __DecompressItemAsync. Bypassing the queue here would break
            // ordering for callers that have prior items in flight.
            if (src == null) src = new byte[0];

            _lz4CallbackReceivers = _lz4CallbackReceivers.Append(self);
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

            if (_lz4Buffer[0] == null || _lz4Buffer[0].Length == 0)
            {
                OnDecompressError(DecompressError.EmptyInput);
                return;
            }

            if (!ValidateData(_lz4Buffer[0], out var contentIndex, out var hasBlockSum, out var hasContentSum,
                    out var hasContentSize, out var maxBlockSize, out var error, out var maxContentSize))
            {
                OnDecompressError(error);
                return;
            }

            if (hasContentSize)
            {
                _lz4MaxSizes[0] = maxContentSize;
            }

            // skip header checksum byte
            if (contentIndex + 1 > _lz4Buffer[0].Length)
            {
                OnDecompressError(DecompressError.InvalidHeader);
                return;
            }
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

            if (_lz4MaxSizes[0] < 0 || _lz4MaxSizes[0] > MAX_OUTPUT_SIZE)
            {
                OnDecompressError(DecompressError.OutputTooLarge);
                return;
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
            if (!TryReadU32(_lz4Buffer[0], ref _lz4SIndex, out var compSize))
            {
                OnDecompressError(DecompressError.InvalidBlock);
                return;
            }

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
                if (_lz4SIndex + 4 > _lz4Buffer[0].Length)
                {
                    OnDecompressError(DecompressError.InvalidBlock);
                    return;
                }
                _lz4SIndex += 4;
            }

            if ((compSize & BS_UNCOMPRESSED) != 0)
            {
                compSize &= ~BS_UNCOMPRESSED;

                if (_lz4SIndex + compSize > _lz4Buffer[0].Length
                    || _lz4DIndex + compSize > _lz4Dist.Length)
                {
                    OnDecompressError(DecompressError.InvalidBlock);
                    return;
                }

                Array.Copy(_lz4Buffer[0], _lz4SIndex, _lz4Dist, _lz4DIndex, compSize);
                _lz4SIndex += compSize;
                _lz4DIndex += compSize;
                __DecompressFrameInternalAsyncEnd();
            }
            else
            {
                _lz4SLength = compSize;
                if (_lz4SIndex + _lz4SLength > _lz4Buffer[0].Length)
                {
                    OnDecompressError(DecompressError.InvalidBlock);
                    return;
                }
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
                var status = DecompressBlockInternal(_lz4Buffer[0], _lz4Dist, ref _lz4SIndex, ref _lz4DIndex);
                if (status == BLOCK_ERROR)
                {
                    OnDecompressError(DecompressError.InvalidBlock);
                    return;
                }
                if (status == BLOCK_END) break;

                if (!(Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)) continue;
                SendCustomEventDelayedFrames(nameof(_DecompressBlockInternalAsync), ASYNC_DELAY);
                return;
            }

            __DecompressFrameInternalAsyncEnd();
        }

        private void __DecompressFrameInternalAsyncEnd()
        {
            if (_lz4HasContentSum)
            {
                if (_lz4SIndex + 4 > _lz4Buffer[0].Length)
                {
                    OnDecompressError(DecompressError.InvalidBlock);
                    return;
                }
                _lz4SIndex += 4;
            }

            if (Time.realtimeSinceStartup - _lz4StartTime > MaxFrameTime)
            {
                SendCustomEventDelayedFrames(nameof(_DecompressFrameInternalAsync), ASYNC_DELAY);
                return;
            }

            __DecompressFrameInternalAsync();
        }

        private void OnDecompressSuccess(byte[] result)
        {
            _lz4LastError = DecompressError.None;
            _lz4CallbackReceivers = _lz4CallbackReceivers.Shift(out var device);
            _lz4DecompressedData = result;
            _lz4Dist = null;
            _lz4Buffer = _lz4Buffer.Shift();
            _lz4MaxSizes = _lz4MaxSizes.Shift();
            if (device != null) device.SendCustomEventDelayedFrames("OnLZ4Decompress", ASYNC_DELAY);
            // Schedule the next item one frame after the receiver wakes up so that
            // GetDecompressedData() / GetLastError() can be read before the next
            // item overwrites them.
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), ASYNC_DELAY + 1);
        }

        private void OnDecompressError(DecompressError error)
        {
            Debug.LogError($"[UdonLZ4] Failed to decompress: {error}");
            _lz4LastError = error;
            _lz4CallbackReceivers = _lz4CallbackReceivers.Shift(out var device);
            _lz4DecompressedData = null;
            _lz4Dist = null;
            _lz4Buffer = _lz4Buffer.Shift();
            _lz4MaxSizes = _lz4MaxSizes.Shift();
            if (device != null) device.SendCustomEventDelayedFrames("OnLZ4DecompressError", ASYNC_DELAY);
            SendCustomEventDelayedFrames(nameof(__DecompressItemAsync), ASYNC_DELAY + 1);
        }

        private bool ValidateData(byte[] src, out long contentIndex, out bool hasBlockSum, out bool hasContentSum,
            out bool hasContentSize, out long maxBlockSize, out DecompressError error, out long maxContentSize)
        {
            contentIndex = -1;
            hasBlockSum = false;
            hasContentSum = false;
            hasContentSize = false;
            maxBlockSize = 0;
            maxContentSize = 0;
            error = DecompressError.None;
            long sIndex = 0;

            if (!TryReadU32(src, ref sIndex, out var magic))
            {
                Debug.LogWarning("invalid header (truncated magic)");
                error = DecompressError.InvalidHeader;
                return false;
            }
            if (magic != MAGIC_NUM)
            {
                Debug.LogWarning("invalid magic number");
                error = DecompressError.InvalidMagicNumber;
                return false;
            }

            if (!TryReadByte(src, ref sIndex, out var descriptor))
            {
                error = DecompressError.InvalidHeader;
                return false;
            }

            if ((descriptor & FD_VERSION_MASK) != FD_VERSION)
            {
                Debug.LogWarning("invalid version");
                error = DecompressError.InvalidVersion;
                return false;
            }

            hasBlockSum = (descriptor & FD_BLOCK_CHKSUM) != 0;
            hasContentSum = (descriptor & FD_CONTENT_CHKSUM) != 0;
            hasContentSize = (descriptor & FD_CONTENT_SIZE) != 0;
            var hasDictId = (descriptor & FD_DICT_ID) != 0;

            // Read block size descriptor
            if (!TryReadByte(src, ref sIndex, out var bsByte))
            {
                error = DecompressError.InvalidHeader;
                return false;
            }
            var bsIdx = (bsByte >> BS_SHIFT) & BS_MASK;
            maxBlockSize = _bsMap[bsIdx];
            if (maxBlockSize == 0)
            {
                Debug.LogWarning("invalid block size");
                error = DecompressError.InvalidBlockSize;
                return false;
            }

            if (hasContentSize)
            {
                if (!TryReadU64(src, ref sIndex, out var rawContentSize))
                {
                    error = DecompressError.InvalidHeader;
                    return false;
                }
                if (rawContentSize > MAX_OUTPUT_SIZE)
                {
                    error = DecompressError.OutputTooLarge;
                    return false;
                }
                maxContentSize = (long)rawContentSize;
            }

            if (hasDictId)
            {
                if (sIndex + 4 > src.Length)
                {
                    error = DecompressError.InvalidHeader;
                    return false;
                }
                sIndex += 4;
            }

            contentIndex = sIndex;
            return true;
        }

        private long DecompressBound(byte[] src, long sIndex, long maxBlockSize, bool hasBlockSum)
        {
            long maxSize = 0;
            while (true)
            {
                if (!TryReadU32(src, ref sIndex, out var blockSize))
                {
                    Debug.LogWarning("invalid block size (truncated)");
                    return 0;
                }

                if (blockSize == 0) return maxSize;

                var realSize = (long)(blockSize & ~BS_UNCOMPRESSED);
                if ((blockSize & BS_UNCOMPRESSED) != 0)
                {
                    maxSize += realSize;
                }
                else
                {
                    maxSize += maxBlockSize;
                }

                if (maxSize < 0 || maxSize > MAX_OUTPUT_SIZE)
                {
                    Debug.LogWarning("output too large");
                    return 0;
                }

                if (hasBlockSum)
                {
                    if (sIndex + 4 > src.Length) return 0;
                    sIndex += 4;
                }

                if (sIndex + realSize > src.Length) return 0;
                sIndex += realSize;
            }
        }

        private int DecompressBlockInternal(byte[] src, byte[] dst, ref long sIndex, ref long dIndex)
        {
            if (!TryReadByte(src, ref sIndex, out var token)) return BLOCK_ERROR;

            long literalCount = token >> 4;
            if (literalCount > 0)
            {
                if (literalCount == 0xf)
                {
                    while (true)
                    {
                        // _lz4SEnd <= src.Length is invariant from __DecompressFrameInternalAsync
                        if (sIndex >= _lz4SEnd) return BLOCK_ERROR;
                        var lenByte = src[sIndex++];
                        literalCount += lenByte;
                        if (literalCount > dst.Length - dIndex) return BLOCK_ERROR;
                        if (lenByte != 0xff) break;
                    }
                }

                if (literalCount < 0
                    || sIndex + literalCount > _lz4SEnd
                    || dIndex + literalCount > dst.Length)
                {
                    return BLOCK_ERROR;
                }

                Array.Copy(src, sIndex, dst, dIndex, literalCount);
                sIndex += literalCount;
                dIndex += literalCount;
            }

            // After the literal copy sIndex is bounded by _lz4SEnd, so equality
            // means the last sequence (literals only) just consumed the block.
            // A strict > would indicate a logic bug elsewhere — surface as error.
            if (sIndex == _lz4SEnd)
            {
                return BLOCK_END;
            }
            if (sIndex > _lz4SEnd)
            {
                return BLOCK_ERROR;
            }

            long mLength = token & 0xf;

            if (sIndex + 2 > _lz4SEnd) return BLOCK_ERROR;
            long mOffset = src[sIndex++] | (src[sIndex++] << 8);

            if (mLength == 0xf)
            {
                while (true)
                {
                    if (sIndex >= _lz4SEnd) return BLOCK_ERROR;
                    var lenByte = src[sIndex++];
                    mLength += lenByte;
                    if (mLength > dst.Length - dIndex) return BLOCK_ERROR;
                    if (lenByte != 0xff) break;
                }
            }

            mLength += MIN_MATCH;

            if (mOffset <= 0
                || mOffset > dIndex
                || mLength > dst.Length - dIndex)
            {
                return BLOCK_ERROR;
            }

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

            return BLOCK_CONTINUE;
        }

        private static bool TryReadByte(byte[] b, ref long n, out byte value)
        {
            if (b == null || n < 0 || n + 1 > b.Length)
            {
                value = 0;
                return false;
            }
            value = b[n];
            n += 1;
            return true;
        }

        private static bool TryReadU32(byte[] b, ref long n, out uint value)
        {
            if (b == null || n < 0 || n + 4 > b.Length)
            {
                value = 0;
                return false;
            }
            value = (uint)b[n]
                  | ((uint)b[n + 1] << 8)
                  | ((uint)b[n + 2] << 16)
                  | ((uint)b[n + 3] << 24);
            n += 4;
            return true;
        }

        private static bool TryReadU64(byte[] b, ref long n, out ulong value)
        {
            if (b == null || n < 0 || n + 8 > b.Length)
            {
                value = 0;
                return false;
            }
            value = (ulong)b[n]
                  | ((ulong)b[n + 1] << 8)
                  | ((ulong)b[n + 2] << 16)
                  | ((ulong)b[n + 3] << 24)
                  | ((ulong)b[n + 4] << 32)
                  | ((ulong)b[n + 5] << 40)
                  | ((ulong)b[n + 6] << 48)
                  | ((ulong)b[n + 7] << 56);
            n += 8;
            return true;
        }

        public byte[] GetDecompressedData()
        {
            return _lz4DecompressedData;
        }

        public DecompressError GetLastError()
        {
            return _lz4LastError;
        }

        public void ClearDecompressedData()
        {
            _lz4DecompressedData = null;
        }
    }
}
