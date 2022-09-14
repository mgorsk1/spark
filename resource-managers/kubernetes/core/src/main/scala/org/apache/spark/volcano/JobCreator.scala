package org.apache.spark.volcano

import io.fabric8.kubernetes.api.model.volcano.batch._
import io.fabric8.kubernetes.api.model.{OwnerReferenceBuilder, Pod}
import org.apache.spark.deploy.k8s.Constants
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils
import org.apache.spark.internal.Logging

import java.util
import java.util.Collections
import scala.collection.JavaConverters._

object JobCreator extends Logging {

  def job(jobName: String, queueName: String, schedulerName: String, namespace: String, maxRetry: Int): JobBuilder = {
    new JobBuilder()
      .withApiVersion("batch.volcano.sh/v1alpha1")
      .withNewSpec()
      .withQueue(queueName)
      .withSchedulerName(schedulerName)
      .withMaxRetry(maxRetry)
      .endSpec()
      .withNewMetadata()
      .withName(jobName)
      .withNamespace(namespace)
      // The uid is created in the basicDiverFeatureStep and has to be attached to the job to enable the owner reference
      // .withUid(pod.getMetadata().getUid())
      .endMetadata()
  }

  private def addDynamicQueueAnnotations(queueHierarchy: String): util.Map[String, String] = {
    val annotations = new util.HashMap[String, String]()
    if (!"".equals(queueHierarchy)) {
      annotations.put(Constants.VOLCANO_ANNOTATION_DYNAMIC_QUEUE_NAME, queueHierarchy)
    }
    annotations
  }

  def driver(driverPod: Pod,
             queueName: String,
             schedulerName: String,
             namespace: String,
             maxRetry: Int,
             queueHierarchy: String
            ): Job = {
    logInfo(s"Creating Driver Job with name: ${KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME}")

    // DriverTask lifecyclePolicy to delay the job completion until the task
    // has completed
    val completedPolicy: LifecyclePolicy = new LifecyclePolicy()
    completedPolicy.setAction("CompleteJob")
    completedPolicy.setEvent("TaskCompleted")

    // Complete the Job if the Driver task fails
    // Otherwise the spark-submit process never terminates if the Driver container
    // fails for some reason
    val failedPolicy: LifecyclePolicy = new LifecyclePolicy()
    failedPolicy.setAction("CompleteJob")
    failedPolicy.setEvent("PodFailed")

    val evictedPolicy: LifecyclePolicy = new LifecyclePolicy()
    evictedPolicy.setAction("CompleteJob")
    evictedPolicy.setEvent("PodEvicted")

//    driverPod.getSpec.setPriorityClassName()

    val driverTask: TaskSpec = new TaskSpecBuilder()
      .withReplicas(1)
      .withName(Constants.SPARK_POD_DRIVER_ROLE)
      .withNewTemplate()
      .withSpec(driverPod.getSpec)
      .withMetadata(driverPod.getMetadata)
      .endTemplate()
      .withPolicies(completedPolicy, failedPolicy, evictedPolicy)
      .build()

    val jobLabels = new java.util.HashMap[String, String]()
    jobLabels.put(Constants.VOLCANO_JOB_NAME_LABEL_KEY, KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME)
    jobLabels.put(Constants.SPARK_ROLE_LABEL, Constants.SPARK_POD_DRIVER_ROLE)
    // Make the driver job non-reclaimable by Volcano HDRF plugin
    jobLabels.put(Constants.VOLCANO_JOB_RECLAIMABLE_LABEL, false.toString)

    val jobAnnotations = addDynamicQueueAnnotations(queueHierarchy)

    // The driver job has only one 1 task and only 1 pod
    // minAvailable must be set to 1 otherwise it goes to "completed" phase
    // as soon as it starts running
    job(KubernetesClientUtils.DRIVER_VOLCANO_JOB_NAME, queueName, schedulerName, namespace, maxRetry)
      .editSpec()
      .withTasks(List(driverTask).asJava)
      .withMinAvailable(1)
      .endSpec()
      .editMetadata()
      .withLabels(jobLabels)
      .withAnnotations(jobAnnotations)
      .endMetadata()
      .build()
  }

  def executor(pod: Pod,
               executorJobName: String,
               queueName: String,
               schedulerName: String,
               namespace: String,
               driverPod: Option[Pod],
               queueHierarchy: String,
               ttlSecondsAfterFinished: Int,
              ): Job = {
    logInfo(s"Creating Executor Job with name: $executorJobName")
    val executorTask: TaskSpec = createExecutorTask(pod)

    val jobAnnotations = addDynamicQueueAnnotations(queueHierarchy)
    jobAnnotations.put(Constants.VOLCANO_DYNAMIC_LABEL, Constants.SPARK_EXECUTOR_ID_LABEL)

    // The executor job is always created with maxRetry = 0
    // this is because executor restarts through K8s or Volcano interfere with
    // spark's internal logic to handle executor loss

    val executorJobBuilder = job(executorJobName, queueName, schedulerName, namespace, 0)
      .editSpec()
      .withMinAvailable(1)
      .withTasks(List(executorTask).asJava)
      .withTtlSecondsAfterFinished(ttlSecondsAfterFinished)
      .endSpec()
      .editMetadata()
      .withAnnotations(jobAnnotations)
      .endMetadata()

    // Add the driver pod as an owner for the executor job so that when the driver pod is deleted
    // all corresponding executor jobs are deleted as well
    if (driverPod.isDefined) {
      val driverPodRef = new OwnerReferenceBuilder()
        .withName(driverPod.get.getMetadata.getName)
        .withApiVersion(driverPod.get.getApiVersion)
        .withUid(driverPod.get.getMetadata.getUid)
        .withKind(driverPod.get.getKind)
        .withController(true)
        .build()

      executorJobBuilder
        .editMetadata()
        .withOwnerReferences(Collections.singletonList(driverPodRef))
        .endMetadata()
        .build()
    } else {
      executorJobBuilder.build()
    }
  }

  private def createExecutorTask(pod: Pod): TaskSpec = {

    val executorTask = new TaskSpecBuilder()
      .withReplicas(1)
      .withName(Constants.SPARK_POD_EXECUTOR_ROLE)
      .withNewTemplate()
      .withSpec(pod.getSpec)
      .withMetadata(pod.getMetadata)
      .endTemplate()
      .build()
    executorTask
  }
}
