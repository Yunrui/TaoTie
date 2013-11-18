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
        /// <summary>
        /// We just simply using this delimiter to separrate value in the dictionary
        /// Serialization is too slow
        /// $TODO: we need to find an elegant way later
        /// </summary>
        private const string delimiter = "____";
        private IList<string> values = new List<string>();

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="values"></param>
        public Tuple(IList<string> values)
        {
            this.values = values;
        }

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="value"></param>
        public Tuple(string value)
        {
            this.values.Add(value);
        }

        public object Get(int index)
        {
            return this.values[index];
        }

        /// <summary>
        /// The first version used BinaryFormatter, the performance penalty is unacceptable.
        /// So I switch back to only support string type and write simple formatter
        /// </summary>
        /// <returns></returns>
        public string GetSeriliableContent()
        {
            return string.Join(Tuple.delimiter, this.values.ToArray());
        }

        public static Tuple Parse(string message)
        {
            var parts = message.Split(new string[] { Tuple.delimiter }, StringSplitOptions.RemoveEmptyEntries);

            return new Tuple(parts.ToList());
        }
    }
}
