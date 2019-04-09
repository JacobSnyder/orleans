using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class BasicErrorHandler : StreamMonitorBase, IErrorHandler
    {
        private StreamSequenceToken highWaterToken;
        private int highWaterCount;
        
        public bool ClassifyPoisonStatus(StreamSequenceToken token, Exception exception)
        {
            if (this.highWaterToken == null)
            {
                this.highWaterToken = token;
                this.highWaterCount = 1;

                return false;
            }

            var comparison = token.EasyCompareTo(this.highWaterToken);

            switch (comparison)
            {
                case EasyCompareToResult.Before:
                {
                    return false;
                }
                case EasyCompareToResult.Equal:
                {
                    ++this.highWaterCount;

                    if (this.highWaterCount < 10) // TODO: Config
                    {
                        return false;
                    }

                    return true;
                }
                case EasyCompareToResult.After:
                {
                    this.highWaterToken = token;
                    this.highWaterCount = 1;

                    return false;
                }
                default:
                {
                    throw new NotSupportedException($"Unknown {nameof(EasyCompareToResult)} '{comparison}'");
                }
            }

        }

        public TimeSpan GetRecoveryBackoff(StreamSequenceToken token, Exception exception)
        {
            return TimeSpan.FromSeconds(5 * (this.highWaterCount - 1));
        }
    }
}