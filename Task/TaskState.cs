using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    public enum ActorState
    {
        /// <summary>
        /// New Actor Thread just created by a Service Role.
        /// It happens when a Service Role is just rolled up or
        /// Previous thread was killed by some reasons
        /// </summary>
        NewBorn = 0, 

        /// <summary>
        /// Actor is taking Queue item assigned to it.
        /// $NOTE: after Actor is assigned, it's never ending until error out
        /// </summary>
        Working = 1,

        /// <summary>
        /// Any unexpected Exceptions causes thread abort
        /// </summary>
        Error = 2,
    }
}
