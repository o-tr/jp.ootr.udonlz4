using UdonSharp;

namespace jp.ootr.UdonLZ4
{
    public enum DecompressError
    {
        None,
        InvalidMagicNumber,
        InvalidVersion,
        InvalidBlockSize,
        InvalidHeader,
        InvalidBlock,
        OutputTooLarge,
        EmptyInput
    }

    public interface ILZ4CallbackReceiver
    {
        void OnLZ4Decompress();
        void OnLZ4DecompressError();
    }

    public class LZ4CallbackReceiver : UdonSharpBehaviour
    {
        public virtual void OnLZ4Decompress()
        {
        }

        public virtual void OnLZ4DecompressError()
        {
        }
    }
}
