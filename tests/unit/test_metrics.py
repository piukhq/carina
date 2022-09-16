import random

from prometheus_client import REGISTRY

from carina.core.config import settings
from carina.tasks.prometheus import METRIC_NAME_PREFIX, task_processing_time_callback_fn


def test_metrics_callback_fn_multiple_metrics() -> None:
    metric_name = f"{METRIC_NAME_PREFIX}tasks_processing_time"
    mock_task_name = "mock-task-name"
    mock_task_processing_times = []

    # Publish 3 metrics to the same labels
    num_of_metrics = 3
    for _ in range(num_of_metrics):
        mock_task_processing_time = random.uniform(0.1, 0.5)
        mock_task_processing_times.append(mock_task_processing_time)
        task_processing_time_callback_fn(mock_task_processing_time, mock_task_name)

    metric_labels = {"app": settings.PROJECT_NAME, "task_name": mock_task_name}
    metric_count = REGISTRY.get_sample_value(name=f"{metric_name}_count", labels=metric_labels)
    metric_value = REGISTRY.get_sample_value(name=f"{metric_name}_sum", labels=metric_labels)

    assert metric_count == num_of_metrics
    assert metric_value == sum(mock_task_processing_times)
