using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrimitiveInterface
{
    /// <summary>
    /// Tuple
    /// </summary>
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

        public byte[] GetSeriliableContent()
        {
        }
    }
}
