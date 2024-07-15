using System;
using UnityEngine;

namespace jp.ootr.UdonLZ4
{
    public static class Utils
    {
        public static T[] Append<T>(this T[] array, T item)
        {
            var tmpArray = new T[array.Length + 1];
            array.CopyTo(tmpArray, 0);
            tmpArray[array.Length] = item;
            return tmpArray;
        }

        public static T[] Remove<T>(this T[] array, int index)
        {
            if (index < 0 || index >= array.Length)
            {
                Debug.LogWarning($"RemoveItemFromArray: Index out of range: {index}, array length: {array.Length}");
                return array;
            }

            var tmpArray = new T[array.Length - 1];
            Array.Copy(array, 0, tmpArray, 0, index);
            Array.Copy(array, index + 1, tmpArray, index, array.Length - index - 1);
            return tmpArray;
        }
    }
}