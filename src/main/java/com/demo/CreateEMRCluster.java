package com.demo;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.Arrays;

public class CreateEMRCluster {
    public static void main(String[] args) {
        // 创建 EMR 客户端
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion("us-west-2")
                .build();
        // 定义应用程序
        Application flink = new Application().withName("Flink");
        Application hadoop = new Application().withName("Hadoop");
        Application hive = new Application().withName("Hive");
        Application spark = new Application().withName("Spark");
        Application zookeeper = new Application().withName("ZooKeeper");

        // 定义实例配置 (实例组 fleets)
        InstanceFleetConfig coreFleet = new InstanceFleetConfig()
                .withName("Core")
                .withInstanceFleetType(InstanceFleetType.CORE)
                .withTargetOnDemandCapacity(8)
                .withLaunchSpecifications(new InstanceFleetProvisioningSpecifications()
                        .withOnDemandSpecification(new OnDemandProvisioningSpecification()
                                .withAllocationStrategy(OnDemandProvisioningAllocationStrategy.LowestPrice)
                                .withCapacityReservationOptions(new OnDemandCapacityReservationOptions()
                                        .withUsageStrategy(OnDemandCapacityReservationUsageStrategy.UseCapacityReservationsFirst)
                                        .withCapacityReservationResourceGroupArn("arn:aws:resource-groups:us-west-2:xxxx:group/crg_targeted"))
                        ))
                .withInstanceTypeConfigs(new InstanceTypeConfig()
                        .withInstanceType("r6g.xlarge")
                        .withWeightedCapacity(4)
                        .withEbsConfiguration(new EbsConfiguration()
                                .withEbsOptimized(true)
                                .withEbsBlockDeviceConfigs(
                                        new EbsBlockDeviceConfig()
                                                .withVolumesPerInstance(1)
                                                .withVolumeSpecification(new VolumeSpecification()
                                                        .withVolumeType("gp3")
                                                        .withSizeInGB(32)))));
        InstanceFleetConfig taskFleet = new InstanceFleetConfig()
                .withName("Task - 1")
                .withInstanceFleetType(InstanceFleetType.TASK)
                .withTargetOnDemandCapacity(48)
                .withTargetSpotCapacity(16)
                .withLaunchSpecifications(new InstanceFleetProvisioningSpecifications()
                        .withOnDemandSpecification(new OnDemandProvisioningSpecification()
                                .withAllocationStrategy(OnDemandProvisioningAllocationStrategy.LowestPrice)
                                .withCapacityReservationOptions(new OnDemandCapacityReservationOptions()
                                        .withUsageStrategy(OnDemandCapacityReservationUsageStrategy.UseCapacityReservationsFirst)
                                        .withCapacityReservationResourceGroupArn("arn:aws:resource-groups:us-west-2:xxxx:group/crg_targeted"))
                        )
                        .withSpotSpecification(new SpotProvisioningSpecification()
                                .withTimeoutAction(SpotProvisioningTimeoutAction.TERMINATE_CLUSTER)
                                .withTimeoutDurationMinutes(60)
                                .withAllocationStrategy(SpotProvisioningAllocationStrategy.CapacityOptimized))
                )
                .withInstanceTypeConfigs(
                        new InstanceTypeConfig().withInstanceType("r7g.4xlarge")
                                .withWeightedCapacity(16)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r7g.2xlarge")
                                .withWeightedCapacity(8)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r6g.4xlarge")
                                .withWeightedCapacity(16)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r5.4xlarge")
                                .withWeightedCapacity(16)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r5.xlarge")
                                .withWeightedCapacity(4)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r5.2xlarge")
                                .withWeightedCapacity(8)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r5.8xlarge")
                                .withWeightedCapacity(32)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32)))),
                        new InstanceTypeConfig().withInstanceType("r6g.2xlarge")
                                .withWeightedCapacity(8)
                                .withEbsConfiguration(new EbsConfiguration()
                                        .withEbsOptimized(true)
                                        .withEbsBlockDeviceConfigs(
                                                new EbsBlockDeviceConfig()
                                                        .withVolumesPerInstance(1)
                                                        .withVolumeSpecification(new VolumeSpecification()
                                                                .withVolumeType("gp3")
                                                                .withSizeInGB(32))))
                );
        InstanceFleetConfig masterFleet = new InstanceFleetConfig()
                .withName("Primary")
                .withInstanceFleetType(InstanceFleetType.MASTER)
                .withTargetOnDemandCapacity(1)
                .withInstanceTypeConfigs(new InstanceTypeConfig()
                        .withInstanceType("m7g.2xlarge")
                        .withWeightedCapacity(1)
                        .withEbsConfiguration(new EbsConfiguration()
                                .withEbsOptimized(true)
                                .withEbsBlockDeviceConfigs(
                                        new EbsBlockDeviceConfig()
                                                .withVolumeSpecification(new VolumeSpecification()
                                                        .withVolumeType("gp2")
                                                        .withSizeInGB(32)))));
        // 创建集群请求
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("odcr")
                .withReleaseLabel("emr-6.10.1")
                .withLogUri("s3://aws-logs-xxxx-us-west-2/elasticmapreduce")
                .withServiceRole("arn:aws:iam::xxxx:role/EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withApplications(Arrays.asList(flink, hadoop, hive, spark, zookeeper))
                .withScaleDownBehavior(ScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION)
                .withEbsRootVolumeSize(50)
                .withInstances(new JobFlowInstancesConfig()
                        .withEmrManagedMasterSecurityGroup("sg-03da1000aa24ca9a6")
                        .withEmrManagedSlaveSecurityGroup("sg-03b9de6e86a4579ed")
                        .withTerminationProtected(true)
                        .withEc2KeyName("org")
                        .withKeepJobFlowAliveWhenNoSteps(true)
                        .withInstanceFleets(Arrays.asList(coreFleet, taskFleet, masterFleet))
                        .withEc2SubnetId("subnet-04dd82c91e0565bfa"));
        // 运行集群
        RunJobFlowResult result = emr.runJobFlow(request);
        System.out.println("Cluster ID: " + result.getJobFlowId());
    }
}
