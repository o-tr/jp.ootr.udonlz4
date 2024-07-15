using UdonSharp;

namespace jp.ootr.UdonLZ4
{
    public enum DecompressError
    {
        None,
        InvalidMagicNumber,
        InvalidVersion,
        InvalidBlockSize
    }

    public interface ILZ4CallbackReceiver
    {
        void OnLZ4Decompress(byte[] data);
        void OnLZ4DecompressError(DecompressError error);
    }

    public class LZ4CallbackReceiver : UdonSharpBehaviour
    {
        public virtual void OnLZ4Decompress(byte[] data)
        {
        }

        public virtual void OnLZ4DecompressError(DecompressError error)
        {
        }
    }
}