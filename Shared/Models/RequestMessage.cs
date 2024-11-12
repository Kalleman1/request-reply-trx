namespace Shared.Models
{
    public class RequestMessage
    {
        public string WorkItemId { get; set; }
        public string TaskDetails { get; set; }
        public WorkItemState State { get; set; } = WorkItemState.Pending;
    }

    public enum WorkItemState
    {
        Pending,
        Committed,
        Failed
    }
}