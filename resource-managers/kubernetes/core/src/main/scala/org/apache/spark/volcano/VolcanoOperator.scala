package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.volcano.batch.Job
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config
import org.apache.spark.internal.Logging
import org.apache.spark.volcano.dsl.VolcanoJobOperationsImpl


class VolcanoOperator(kubernetesClient: KubernetesClient, sparkConf: SparkConf) extends Logging {

  val jobClient: VolcanoJobOperationsImpl =
    new VolcanoJobOperationsImpl(
      // the instance of KubernetesClient has to be explicitly cast into the
      // Child class DefaultKubernetesClient to extract the okHTTPClient.
      // This is fine because it is always a DefaultKubernetesClient - see
      // SparkKubernetesClientFactory.createKubernetesClient
      kubernetesClient.asInstanceOf[DefaultKubernetesClient].getHttpClient,
      kubernetesClient.getConfiguration
    )

  private def getQueueHierarchy: String = {
    // If the dynamic queue prefix config option is set, concatenate the prefix and the queue name as the dynamic
    // queue hierarchy annotation
    val queuePrefix = sparkConf.get(Config.KUBERNETES_VOLCANO_DYNAMIC_QUEUE_PREFIX)
    val queueName = sparkConf.get(Config.KUBERNETES_VOLCANO_QUEUE)
    val dynamicQueueHierarchy = if ("".equals(queuePrefix)) "" else s"$queuePrefix/$queueName"
    dynamicQueueHierarchy
  }
  def createDriver(driverPod: Pod): Job = {
    val queueName = sparkConf.get(Config.KUBERNETES_VOLCANO_QUEUE)
    val dynamicQueueHierarchy = getQueueHierarchy

    val resolvedDriverJob = JobCreator.driver(
      driverPod,
      queueName,
      sparkConf.get(Config.KUBERNETES_VOLCANO_SCHEDULER),
      sparkConf.get(Config.KUBERNETES_NAMESPACE),
      sparkConf.get(Config.KUBERNETES_VOLCANO_MAX_RETRY),
      dynamicQueueHierarchy
    )
    jobClient.create(resolvedDriverJob)
  }

  def getPods(podName: String): Pod = {
    kubernetesClient.pods.withName(podName).get()
  }

  def createExecutors(executorJobName: String,
                      executorPod: Pod,
                      driverPod: Option[Pod]
                     ): Job = {
    val queueName = sparkConf.get(Config.KUBERNETES_VOLCANO_QUEUE)
    val dynamicQueueHierarchy = getQueueHierarchy
    val createdJob = JobCreator.executor(
      executorPod,
      executorJobName,
      queueName,
      sparkConf.get(Config.KUBERNETES_VOLCANO_SCHEDULER),
      sparkConf.get(Config.KUBERNETES_NAMESPACE),
      driverPod,
      dynamicQueueHierarchy,
      sparkConf.get(Config.KUBERNETES_VOLCANO_TTL_SECONDS_AFTER_FINISHED),
    )

    jobClient.create(createdJob)
  }

  def delete(job: Job): Boolean = {
    jobClient.delete(job)
  }
}
