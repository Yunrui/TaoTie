using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Task
{
    /// <summary>
    /// Logger for a Scene
    /// </summary>
    /// <remarks>
    /// This is correlation Id for this scene, every following log should use this id to correlate
    /// </remarks>
    class RoundLogger
    {
        [ThreadStatic]
        private static RoundLogger instance;

        /// <summary>
        /// SceneId
        /// </summary>
        private string sceneId = string.Empty;

        /// <summary>
        /// ctor
        /// </summary>
        protected RoundLogger()
        {
        }

        public void Log(string message)
        {
            if (string.IsNullOrEmpty(this.sceneId))
            {
                throw new InvalidOperationException("Scene is not Initialized.");
            }

            Trace.TraceInformation("{0}{1}", this.sceneId, message);
        }

        public void SetSceneId(Actor actor)
        {
            this.sceneId = string.Format("###{0}##{1}##{2}###", actor.DeploymentId, actor.Id, Guid.NewGuid());
        }

        public static RoundLogger Current
        {
            get
            {
                if (RoundLogger.instance == null)
                {
                    RoundLogger.instance = new RoundLogger();
                }

                return RoundLogger.instance;
            }
        }
    }
}
