using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace PrimitiveInterface
{
    /// <summary>
    /// Tuple
    /// </summary>
    [Serializable]
    public class Tuple
    {
        private IList<object> values = new List<object>();

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="values"></param>
        public Tuple(IList<object> values)
        {
            this.values = values;
        }

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="value"></param>
        public Tuple(object value)
        {
            this.values.Add(value);
        }

        public object Get(int index)
        {
            return this.values[index];
        }

        /// <summary>
        /// $TODO: what's performance penalty for this? 
        /// </summary>
        /// <returns></returns>
        public byte[] GetSeriliableContent()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                IFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, this);
                return stream.GetBuffer();
            }
        }

        public static Tuple Parse(byte[] content)
        {
            using (MemoryStream stream = new MemoryStream(content))
            {
                IFormatter formatter = new BinaryFormatter();
                return formatter.Deserialize(stream) as Tuple;
            }
        }
    }
}
