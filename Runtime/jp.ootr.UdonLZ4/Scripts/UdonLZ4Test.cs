using System;
using UnityEngine;
using UnityEngine.UI;
using VRC.SDK3.StringLoading;
using VRC.SDKBase;
using VRC.Udon.Common.Interfaces;

namespace jp.ootr.UdonLZ4
{
    public class UdonLZ4Test : LZ4CallbackReceiver
    {
        public VRCUrl url;
        public UdonLZ4 lz4;
        public RawImage image;

        public override void Interact()
        {
            Load();
        }

        public void Load()
        {
            VRCStringDownloader.LoadUrl(url, (IUdonEventReceiver)this);
        }

        public override void OnStringLoadSuccess(IVRCStringDownload result)
        {
            Debug.Log($"ZipLoader: text-zip loaded successfully from {result.Url}.");
            var data = Convert.FromBase64String(result.Result);
            lz4.DecompressAsync((ILZ4CallbackReceiver)this, data);
        }

        public override void OnLZ4Decompress(byte[] data)
        {
            var texture = new Texture2D(1024, 576);
            texture.LoadRawTextureData(data);
            texture.Apply();
            image.texture = texture;
        }
    }
}
