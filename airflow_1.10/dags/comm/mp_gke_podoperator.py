
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


def make_mp_model_pod_op(task_id: str, image: str, dag: DAG):
    pod_resources = Resources()
    pod_resources.request_cpu = '1000m'
    pod_resources.request_memory = '2048Mi'
    pod_resources.limit_cpu = '2000m'
    pod_resources.limit_memory = '4096Mi'
    pod_resources.limit_gpu = 0

    mp_pod_op = KubernetesPodOperator(task_id=task_id,
                                      namespace='mp-analysis',
                                      # image='test/image',
                                      image=image,
                                      # image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')],
                                      name="job_02-04",  # -> job-02-04
                                      is_delete_operator_pod=True,
                                      get_logs=True,
                                      resources=pod_resources,
                                      # env_from=configmaps,
                                      dag=dag,
                                      )

    return mp_pod_op


