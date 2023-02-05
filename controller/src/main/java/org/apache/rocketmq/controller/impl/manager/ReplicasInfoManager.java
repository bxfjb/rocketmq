/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.controller.impl.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.elect.ElectPolicy;
import org.apache.rocketmq.controller.helper.BrokerValidPredicate;
import org.apache.rocketmq.controller.impl.event.AlterSyncStateSetEvent;
import org.apache.rocketmq.controller.impl.event.ApplyBrokerIdEvent;
import org.apache.rocketmq.controller.impl.event.CleanBrokerDataEvent;
import org.apache.rocketmq.controller.impl.event.ControllerResult;
import org.apache.rocketmq.controller.impl.event.ElectMasterEvent;
import org.apache.rocketmq.controller.impl.event.EventMessage;
import org.apache.rocketmq.controller.impl.event.EventType;
import org.apache.rocketmq.controller.impl.event.UpdateBrokerAddressEvent;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterSuccessRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.register.RegisterSuccessResponseHeader;


/**
 * The manager that manages the replicas info for all brokers. We can think of this class as the controller's memory
 * state machine It should be noted that this class is not thread safe, and the upper layer needs to ensure that it can
 * be called sequentially
 */
public class ReplicasInfoManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private final ControllerConfig controllerConfig;
    private final Map<String/* brokerName */, BrokerReplicaInfo> replicaInfoTable;
    private final Map<String/* brokerName */, SyncStateInfo> syncStateSetInfoTable;

    public ReplicasInfoManager(final ControllerConfig config) {
        this.controllerConfig = config;
        this.replicaInfoTable = new HashMap<>();
        this.syncStateSetInfoTable = new HashMap<>();
    }

    public ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(
            final AlterSyncStateSetRequestHeader request, final SyncStateSet syncStateSet,
            final BrokerValidPredicate brokerAlivePredicate) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<AlterSyncStateSetResponseHeader> result = new ControllerResult<>(new AlterSyncStateSetResponseHeader());
        final AlterSyncStateSetResponseHeader response = result.getResponse();

        if (!isContainsBroker(brokerName)) {
            result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, "Broker metadata is not existed");
            return result;
        }
        final Set<Long> newSyncStateSet = syncStateSet.getSyncStateSet();
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);

        // Check whether the oldSyncStateSet is equal with newSyncStateSet
        final Set<Long> oldSyncStateSet = syncStateInfo.getSyncStateSet();
        if (oldSyncStateSet.size() == newSyncStateSet.size() && oldSyncStateSet.containsAll(newSyncStateSet)) {
            String err = "The newSyncStateSet is equal with oldSyncStateSet, no needed to update syncStateSet";
            LOGGER.warn("{}", err);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
            return result;
        }

        // Check master
        if (!syncStateInfo.getMasterBrokerId().equals(request.getMasterBrokerId())) {
            String err = String.format("Rejecting alter syncStateSet request because the current leader is:{%s}, not {%s}",
                    syncStateInfo.getMasterBrokerId(), request.getMasterBrokerId());
            LOGGER.error("{}", err);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_MASTER, err);
            return result;
        }

        // Check master epoch
        if (request.getMasterEpoch() != syncStateInfo.getMasterEpoch()) {
            String err = String.format("Rejecting alter syncStateSet request because the current master epoch is:{%d}, not {%d}",
                    syncStateInfo.getMasterEpoch(), request.getMasterEpoch());
            LOGGER.error("{}", err);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_MASTER_EPOCH, err);
            return result;
        }

        // Check syncStateSet epoch
        if (syncStateSet.getSyncStateSetEpoch() != syncStateInfo.getSyncStateSetEpoch()) {
            String err = String.format("Rejecting alter syncStateSet request because the current syncStateSet epoch is:{%d}, not {%d}",
                    syncStateInfo.getSyncStateSetEpoch(), syncStateSet.getSyncStateSetEpoch());
            LOGGER.error("{}", err);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH, err);
            return result;
        }

        // Check newSyncStateSet correctness
        for (Long replica : newSyncStateSet) {
            if (!brokerReplicaInfo.isBrokerExist(replica)) {
                String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't exist", replica);
                LOGGER.error("{}", err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REPLICAS, err);
                return result;
            }
            if (!brokerAlivePredicate.check(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName(), replica)) {
                String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't alive", replica);
                LOGGER.error(err);
                result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NOT_ALIVE, err);
                return result;
            }
        }

        if (!newSyncStateSet.contains(syncStateInfo.getMasterBrokerId())) {
            String err = String.format("Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {%s}", syncStateInfo.getMasterBrokerId());
            LOGGER.error(err);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
            return result;
        }

        // Generate event
        int epoch = syncStateInfo.getSyncStateSetEpoch() + 1;
        response.setNewSyncStateSetEpoch(epoch);
        result.setBody(new SyncStateSet(newSyncStateSet, epoch).encode());
        final AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, newSyncStateSet);
        result.addEvent(event);
        return result;
    }

    public ControllerResult<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request,
                                                                   final ElectPolicy electPolicy) {
        final String brokerName = request.getBrokerName();
        final Long brokerId = request.getBrokerId();
        final ControllerResult<ElectMasterResponseHeader> result = new ControllerResult<>(new ElectMasterResponseHeader());
        final ElectMasterResponseHeader response = result.getResponse();
        if (!isContainsBroker(brokerName)) {
            // this broker set hasn't been registered
            response.setMasterAddress("");
            result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, "Broker hasn't been registered");
            return result;
        }

        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final Set<Long> syncStateSet = syncStateInfo.getSyncStateSet();
        final Long oldMaster = syncStateInfo.getMasterBrokerId();
        Set<Long> allReplicaBrokers = controllerConfig.isEnableElectUncleanMaster() ? brokerReplicaInfo.getAllBroker() : null;
        Long newMaster = null;

        if (syncStateInfo.isFirstTimeForElect()) {
            // If never have a master in this broker set, in other words, it is the first time to elect a master
            // elect it as the first master
            newMaster = brokerId;
        }

        // elect by policy
        if (newMaster == null) {
            // we should assign this assignedBrokerAddr when the brokerAddress need to be elected by force
            Long assignedBrokerId = request.isForceElect() ? brokerId : null;
            newMaster = electPolicy.elect(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName(), syncStateSet, allReplicaBrokers, oldMaster, assignedBrokerId);
        }

        if (newMaster != null && newMaster.equals(oldMaster)) {
            // old master still valid, change nothing
            String err = String.format("The old master %s is still alive, not need to elect new master for broker %s", oldMaster, brokerReplicaInfo.getBrokerName());
            LOGGER.warn("{}", err);
            // the master still exist
            response.setMasterEpoch(syncStateInfo.getMasterEpoch());
            response.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
            response.setMasterBrokerId(oldMaster);
            response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(oldMaster));
            result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, err);
            return result;
        }

        // a new master is elected
        if (newMaster != null) {
            final int masterEpoch = syncStateInfo.getMasterEpoch();
            final int syncStateSetEpoch = syncStateInfo.getSyncStateSetEpoch();
            response.setMasterBrokerId(newMaster);
            response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(newMaster));
            response.setMasterEpoch(masterEpoch + 1);
            response.setSyncStateSetEpoch(syncStateSetEpoch + 1);
            BrokerMemberGroup brokerMemberGroup = buildBrokerMemberGroup(brokerName);
            if (null != brokerMemberGroup) {
                response.setBrokerMemberGroup(brokerMemberGroup);
                result.setBody(brokerMemberGroup.encode());
            }
            final ElectMasterEvent event = new ElectMasterEvent(brokerName, newMaster);
            result.addEvent(event);
            return result;
        }
        // If elect failed and the electMaster is triggered by controller (we can figure it out by brokerAddress),
        // we still need to apply an ElectMasterEvent to tell the statemachine
        // that the master was shutdown and no new master was elected.
        if (request.getBrokerId() == null) {
            final ElectMasterEvent event = new ElectMasterEvent(false, brokerName);
            result.addEvent(event);
            result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, "Old master has down and failed to elect a new broker master");
        } else {
            result.setCodeAndRemark(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, "Failed to elect a new master");
        }
        response.setMasterAddress("");
        return result;
    }

    private BrokerMemberGroup buildBrokerMemberGroup(final String brokerName) {
        if (isContainsBroker(brokerName)) {
            final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
            final BrokerMemberGroup group = new BrokerMemberGroup(brokerReplicaInfo.getClusterName(), brokerName);
            final HashMap<Long, String> brokerIdTable = brokerReplicaInfo.getBrokerIdTable();
            final HashMap<Long, String> memberGroup = new HashMap<>();
            brokerIdTable.forEach((id, addr) -> memberGroup.put(id, addr));
            group.setBrokerAddrs(memberGroup);
            return group;
        }
        return null;
    }

//    public ControllerResult<RegisterBrokerToControllerResponseHeader> registerBroker(
//            final RegisterBrokerToControllerRequestHeader request, final BrokerValidPredicate alivePredicate) {
//        String brokerAddress = request.getBrokerAddress();
//        final String brokerName = request.getBrokerName();
//        final String clusterName = request.getClusterName();
//        final ControllerResult<RegisterBrokerToControllerResponseHeader> result = new ControllerResult<>(new RegisterBrokerToControllerResponseHeader());
//        final RegisterBrokerToControllerResponseHeader response = result.getResponse();
//        if (!isContainsBroker(brokerName)) {
//            result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, "Broker-set hasn't been registered in controller");
//            return result;
//        }
//        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
//        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
//        if (brokerReplicaInfo.isBrokerExist())
//
//        // If the broker's metadata does not exist in the state machine, we can assign the broker a brokerId valued 1
//        // By default, we set this variable to a value of 1
//        long brokerId = MixAll.FIRST_SLAVE_ID;
//        boolean shouldApplyBrokerId = true;
//        if (isContainsBroker(brokerName)) {
//            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
//            final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
//
//            if (brokerReplicaInfo.isBrokerExist(brokerAddress)) {
//                // this broker have registered
//                brokerId = brokerReplicaInfo.getBrokerId(brokerAddress);
//                shouldApplyBrokerId = false;
//            } else {
//                // If this broker replicas is first time come online, we need to apply a new id for this replicas.
//                brokerId = brokerReplicaInfo.newBrokerId();
//            }
//
//            if (syncStateInfo.isMasterExist() && brokerAlivePredicate.test(clusterName, syncStateInfo.getMasterAddress())) {
//                // If the master is alive, just return master info.
//                final String masterAddress = syncStateInfo.getMasterAddress();
//                response.setMasterAddress(masterAddress);
//                response.setMasterEpoch(syncStateInfo.getMasterEpoch());
//                response.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
//            }
//        }
//
//        response.setBrokerId(brokerId);
//        if (response.getMasterAddress() == null) {
//            response.setMasterAddress("");
//        }
//        if (shouldApplyBrokerId) {
//            final ApplyBrokerIdEvent applyIdEvent = new ApplyBrokerIdEvent(request.getClusterName(), brokerName, brokerAddress, brokerId);
//            result.addEvent(applyIdEvent);
//        }
//        return result;
//    }

    public ControllerResult<GetNextBrokerIdResponseHeader> getNextBrokerId(final GetNextBrokerIdRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final ControllerResult<GetNextBrokerIdResponseHeader> result = new ControllerResult<>(new GetNextBrokerIdResponseHeader(clusterName, brokerName));
        final GetNextBrokerIdResponseHeader response = result.getResponse();
        if (brokerReplicaInfo == null) {
            // means that none of brokers in this broker-set are registered
            response.setNextBrokerId(MixAll.FIRST_SLAVE_ID);
        } else {
            response.setNextBrokerId(brokerReplicaInfo.getNextAssignBrokerId());
        }
        return result;
    }

    public ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerId(final ApplyBrokerIdRequestHeader request) {
        final String clusterName = request.getClusterName();
        final String brokerName = request.getBrokerName();
        final Long brokerId = request.getAppliedBrokerId();
        final String registerCheckCode = request.getRegisterCheckCode();
        final String brokerAddress = registerCheckCode.split(";")[0];
        BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final ControllerResult<ApplyBrokerIdResponseHeader> result = new ControllerResult<>(new ApplyBrokerIdResponseHeader(clusterName, brokerName));
        final ApplyBrokerIdEvent event = new ApplyBrokerIdEvent(clusterName, brokerName, brokerAddress, brokerId, registerCheckCode);
        // broker-set unregistered
        if (brokerReplicaInfo == null) {
            // first brokerId
            if (brokerId == MixAll.FIRST_SLAVE_ID) {
                result.addEvent(event);
            } else {
                result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_ID_INVALID, String.format("Broker-set: %s hasn't been registered in controller, but broker try to apply brokerId: %d", brokerName, brokerId));
            }
            return result;
        }
        // broker-set registered
        if (!brokerReplicaInfo.isBrokerExist(brokerId) || registerCheckCode.equals(brokerReplicaInfo.getBrokerRegisterCheckCode(brokerId))) {
            // if brokerId hasn't been assigned or brokerId was assigned to this broker
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_ID_INVALID, String.format("Fail to apply brokerId: %d in broker-set: %s", brokerId, brokerName));
        return result;
    }

    public ControllerResult<RegisterSuccessResponseHeader> registerSuccess(final RegisterSuccessRequestHeader request, final BrokerValidPredicate alivePredicate) {
        final String brokerAddress = request.getBrokerAddress();
        final String brokerName = request.getBrokerName();
        final String clusterName = request.getClusterName();
        final Long brokerId = request.getBrokerId();
        final ControllerResult<RegisterSuccessResponseHeader> result = new ControllerResult<>(new RegisterSuccessResponseHeader(clusterName, brokerName));
        final RegisterSuccessResponseHeader response = result.getResponse();
        if (!isContainsBroker(brokerName)) {
            result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, String.format("Broker-set: %s hasn't been registered in controller", brokerName));
            return result;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        if (!brokerReplicaInfo.isBrokerExist(brokerId)) {
            result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, String.format("BrokerId: %d hasn't been registered in broker-set: %s", brokerId, brokerName));
            return result;
        }
        if (syncStateInfo.isMasterExist() && alivePredicate.check(clusterName, brokerName, syncStateInfo.getMasterBrokerId())) {
            // if master still exist
            response.setMasterBrokerId(syncStateInfo.getMasterBrokerId());
            response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(response.getMasterBrokerId()));
        }
        // if this broker's address has been changed, we need to update it
        if (!brokerAddress.equals(brokerReplicaInfo.getBrokerAddress(brokerId))) {
            final UpdateBrokerAddressEvent event = new UpdateBrokerAddressEvent(clusterName, brokerName, brokerAddress, brokerId);
            result.addEvent(event);
        }
        return result;
    }

    public ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
        final String brokerName = request.getBrokerName();
        final ControllerResult<GetReplicaInfoResponseHeader> result = new ControllerResult<>(new GetReplicaInfoResponseHeader());
        final GetReplicaInfoResponseHeader response = result.getResponse();
        if (isContainsBroker(brokerName)) {
            // If exist broker metadata, just return metadata
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
            final Long masterBrokerId = syncStateInfo.getMasterBrokerId();
            response.setMasterBrokerId(masterBrokerId);
            response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(masterBrokerId));
            response.setMasterEpoch(syncStateInfo.getMasterEpoch());
            result.setBody(new SyncStateSet(syncStateInfo.getSyncStateSet(), syncStateInfo.getSyncStateSetEpoch()).encode());
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST, "Broker metadata is not existed");
        return result;
    }

    public ControllerResult<Void> getSyncStateData(final List<String> brokerNames) {
        final ControllerResult<Void> result = new ControllerResult<>();
        final BrokerReplicasInfo brokerReplicasInfo = new BrokerReplicasInfo();
        for (String brokerName : brokerNames) {
            if (isContainsBroker(brokerName)) {
                // If exist broker metadata, just return metadata
                final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
                final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
                final Set<Long> syncStateSet = syncStateInfo.getSyncStateSet();
                final Long masterBrokerId = syncStateInfo.getMasterBrokerId();
                final ArrayList<BrokerReplicasInfo.ReplicaIdentity> inSyncReplicas = new ArrayList<>();
                final ArrayList<BrokerReplicasInfo.ReplicaIdentity> notInSyncReplicas = new ArrayList<>();

                brokerReplicaInfo.getBrokerIdTable().forEach((brokerId, brokerAddress) -> {
                    if (syncStateSet.contains(brokerId)) {
                        inSyncReplicas.add(new BrokerReplicasInfo.ReplicaIdentity(brokerName, brokerId, brokerAddress));
                    } else {
                        notInSyncReplicas.add(new BrokerReplicasInfo.ReplicaIdentity(brokerName, brokerId, brokerAddress));
                    }
                });

                final BrokerReplicasInfo.ReplicasInfo inSyncState = new BrokerReplicasInfo.ReplicasInfo(masterBrokerId, brokerReplicaInfo.getBrokerAddress(masterBrokerId), syncStateInfo.getMasterEpoch(), syncStateInfo.getSyncStateSetEpoch(), inSyncReplicas, notInSyncReplicas);
                brokerReplicasInfo.addReplicaInfo(brokerName, inSyncState);
            }
        }
        result.setBody(brokerReplicasInfo.encode());
        return result;
    }

    public ControllerResult<Void> cleanBrokerData(final CleanControllerBrokerDataRequestHeader requestHeader,
                                                  final BrokerValidPredicate validPredicate) {
        final ControllerResult<Void> result = new ControllerResult<>();

        final String clusterName = requestHeader.getClusterName();
        final String brokerName = requestHeader.getBrokerName();
        final String brokerIdSetToClean = requestHeader.getBrokerIdSetToClean();

        Set<Long> brokerIdSet = null;
        if (!requestHeader.isCleanLivingBroker()) {
            //if SyncStateInfo.masterAddress is not empty, at least one broker with the same BrokerName is alive
            SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            if (StringUtils.isBlank(brokerIdSetToClean) && null != syncStateInfo && syncStateInfo.getMasterBrokerId() != null) {
                String remark = String.format("Broker %s is still alive, clean up failure", requestHeader.getBrokerName());
                result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark);
                return result;
            }
            if (StringUtils.isNotBlank(brokerIdSetToClean)) {
                brokerIdSet = Stream.of(brokerIdSetToClean.split(";")).map(idStr -> Long.valueOf(idStr)).collect(Collectors.toSet());
                for (Long brokerId : brokerIdSet) {
                    if (validPredicate.check(clusterName, brokerName, brokerId)) {
                        String remark = String.format("Broker [%s,  %s] is still alive, clean up failure", requestHeader.getBrokerName(), brokerId);
                        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark);
                        return result;
                    }
                }
            }
        }
        if (isContainsBroker(brokerName)) {
            final CleanBrokerDataEvent event = new CleanBrokerDataEvent(brokerName, brokerIdSet);
            result.addEvent(event);
            return result;
        }
        result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, String.format("Broker %s is not existed,clean broker data failure.", brokerName));
        return result;
    }

    /**
     * Apply events to memory statemachine.
     *
     * @param event event message
     */
    public void applyEvent(final EventMessage event) {
        final EventType type = event.getEventType();
        switch (type) {
            case ALTER_SYNC_STATE_SET_EVENT:
                handleAlterSyncStateSet((AlterSyncStateSetEvent) event);
                break;
            case APPLY_BROKER_ID_EVENT:
                handleApplyBrokerId((ApplyBrokerIdEvent) event);
                break;
            case ELECT_MASTER_EVENT:
                handleElectMaster((ElectMasterEvent) event);
                break;
            case CLEAN_BROKER_DATA_EVENT:
                handleCleanBrokerDataEvent((CleanBrokerDataEvent) event);
                break;
            case UPDATE_BROKER_ADDRESS:
                handleUpdateBrokerAddress((UpdateBrokerAddressEvent) event);
                break;
            default:
                break;
        }
    }

    private void handleAlterSyncStateSet(final AlterSyncStateSetEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
            syncStateInfo.updateSyncStateSetInfo(event.getNewSyncStateSet());
        }
    }

    private void handleApplyBrokerId(final ApplyBrokerIdEvent event) {
        final String brokerName = event.getBrokerName();
        if (isContainsBroker(brokerName)) {
            final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
            if (!brokerReplicaInfo.isBrokerExist(event.getNewBrokerId())) {
                brokerReplicaInfo.addBroker(event.getNewBrokerId(), event.getBrokerAddress(), event.getRegisterCheckCode());
            }
        } else {
            // First time to register in this broker set
            // Initialize the replicaInfo about this broker set
            final String clusterName = event.getClusterName();
            final BrokerReplicaInfo brokerReplicaInfo = new BrokerReplicaInfo(clusterName, brokerName);
            brokerReplicaInfo.addBroker(event.getNewBrokerId(), event.getBrokerAddress(), event.getRegisterCheckCode());
            this.replicaInfoTable.put(brokerName, brokerReplicaInfo);
            final SyncStateInfo syncStateInfo = new SyncStateInfo(clusterName, brokerName);
            // Initialize an empty syncStateInfo for this broker set
            this.syncStateSetInfoTable.put(brokerName, syncStateInfo);
        }
    }

    private void handleUpdateBrokerAddress(final UpdateBrokerAddressEvent event) {
        final String brokerName = event.getBrokerName();
        final String brokerAddress = event.getBrokerAddress();
        final Long brokerId = event.getBrokerId();
        BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        brokerReplicaInfo.updateBrokerAddress(brokerId, brokerAddress);
    }

    private void handleElectMaster(final ElectMasterEvent event) {
        final String brokerName = event.getBrokerName();
        final Long newMaster = event.getNewMasterBrokerId();
        if (isContainsBroker(brokerName)) {
            final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);

            if (event.getNewMasterElected()) {
                // Record new master
                syncStateInfo.updateMasterInfo(newMaster);

                // Record new newSyncStateSet list
                final HashSet<Long> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(newMaster);
                syncStateInfo.updateSyncStateSetInfo(newSyncStateSet);
            } else {
                // If new master was not elected, which means old master was shutdown and the newSyncStateSet list had no more replicas
                // So we should delete old master, but retain newSyncStateSet list.
                syncStateInfo.updateMasterInfo(null);
            }
            return;
        }
        LOGGER.error("Receive an ElectMasterEvent which contains the un-registered broker, event = {}", event);
    }

    private void handleCleanBrokerDataEvent(final CleanBrokerDataEvent event) {

        final String brokerName = event.getBrokerName();
        final Set<Long> brokerIdSetToClean = event.getBrokerIdSetToClean();

        if (null == brokerIdSetToClean || brokerIdSetToClean.isEmpty()) {
            this.replicaInfoTable.remove(brokerName);
            this.syncStateSetInfoTable.remove(brokerName);
            return;
        }
        if (!isContainsBroker(brokerName)) {
            return;
        }
        final BrokerReplicaInfo brokerReplicaInfo = this.replicaInfoTable.get(brokerName);
        final SyncStateInfo syncStateInfo = this.syncStateSetInfoTable.get(brokerName);
        for (Long brokerId : brokerIdSetToClean) {
            brokerReplicaInfo.removeBrokerId(brokerId);
            syncStateInfo.removeFromSyncState(brokerId);
        }
        if (brokerReplicaInfo.getBrokerIdTable().isEmpty()) {
            this.replicaInfoTable.remove(brokerName);
        }
        if (syncStateInfo.getSyncStateSet().isEmpty()) {
            this.syncStateSetInfoTable.remove(brokerName);
        }
    }

    /**
     * Is the broker existed in the memory metadata
     *
     * @return true if both existed in replicaInfoTable and inSyncReplicasInfoTable
     */
    private boolean isContainsBroker(final String brokerName) {
        return this.replicaInfoTable.containsKey(brokerName) && this.syncStateSetInfoTable.containsKey(brokerName);
    }

}
