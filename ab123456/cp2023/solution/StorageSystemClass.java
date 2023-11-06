package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.*;

import java.util.*;
import java.util.concurrent.Semaphore;

public class StorageSystemClass implements StorageSystem {
    //Map containing data structure with data for every device.
    Map<DeviceId, DeviceDataWrapper> deviceData;
    //Map used to keep track of the ongoing transfers on each component.
    private Map<ComponentId, Boolean> componentsStates;
    private Semaphore transferMUTEX = new Semaphore(1, true);

    public StorageSystemClass(Map<DeviceId, Integer> deviceTotalSlots,
                              Map<ComponentId, DeviceId> componentPlacement) {
        //Check whether there is a component assigned to the device without
        //defined size, or if there are too many components assigned
        //to one device.
        Map<DeviceId, ArrayList<ComponentId>> componentsInDevice = new HashMap<>();
        for (Map.Entry<ComponentId, DeviceId> me :
                componentPlacement.entrySet()) {
            if (!deviceTotalSlots.containsKey(me.getValue())) {
                throw new IllegalArgumentException(
                        "Component assigned to the device " +
                                "without specified size.");
            }
            if(!componentPlacement.containsKey(me.getValue())){
                componentsInDevice.put(me.getValue(), new ArrayList<ComponentId>());
            }
            componentsInDevice.get(me.getValue()).add(me.getKey());
            if (componentsInDevice.get(me.getValue()).size() >
                    deviceTotalSlots.get(me.getValue())) {
                throw new IllegalArgumentException(
                        "Too many components assigned to the device" +
                                "with id = " + me.getValue());
            }
        }
        //Passed parameters were valid, we can initialize our object.
        deviceData = new HashMap<>();
        for(Map.Entry<DeviceId, Integer> entry : deviceTotalSlots.entrySet()){
            if(componentsInDevice.containsKey(entry.getKey())){
                deviceData.put(entry.getKey(), new DeviceDataWrapper(
                        componentsInDevice.get(entry.getKey()), entry.getValue()));
            }
            else{
                deviceData.put(entry.getKey(), new DeviceDataWrapper(
                        new ArrayList<>(), entry.getValue()));
            }
        }
        //initialize mutexex for each component;
        componentsStates = new HashMap<>();
        for(Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()){
            componentsStates.put(entry.getKey(), false);
        }
        //Initialize map containing info about
    }

    private void acquireTransferMutex(){
        try {
            transferMUTEX.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    private void setCompStateToFalse(ComponentId id){
        acquireTransferMutex();
        componentsStates.put(id, false);
        transferMUTEX.release();
    }

    //Validates transfer, and if there's anythong wrong, this functionn
    //released transferMutex and throws a corresponding exception.
    private void validateTransfer(ComponentTransfer transfer) throws TransferException {
        if(componentsStates.containsKey(transfer.getComponentId()) &&
                componentsStates.get(transfer.getComponentId())){
            //There's already an ongoing transfer on the given component.
            transferMUTEX.release();
            throw new ComponentIsBeingOperatedOn(transfer.getComponentId());
        }
        else if(transfer.getSourceDeviceId() != null &&
                !deviceData.containsKey(transfer.getSourceDeviceId())){
            //Source device does not exist.
            transferMUTEX.release();
            throw new DeviceDoesNotExist(transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                !deviceData.containsKey(transfer.getDestinationDeviceId())){
            //Destination device does not exist.
            transferMUTEX.release();
            throw new DeviceDoesNotExist(transfer.getDestinationDeviceId());
        }
        else if(transfer.getSourceDeviceId() != null &&
                !deviceData.get(transfer.getSourceDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEX.release();
            throw new ComponentDoesNotExist(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                deviceData.get(transfer.getDestinationDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEX.release();
            throw new ComponentDoesNotNeedTransfer(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
    }

    private void moveComponentWithFreeMemorySpace(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        //Add component to the list of components
        //leaving the source device.
        deviceData.get(src).addComponentLeavingDevice(comp);
        //Update the number of free memory slots in the dest device.
        deviceData.get(dest).decrementFreeSpace();
        //Release mutex, so other processes can perform their executes
        // in a concurrent manner.
        transferMUTEX.release();
        transfer.prepare();
        //We finished the preparation phrase, now we need to enter
        //the critical section again, so we can perform necessary
        //transition of the component.
        acquireTransferMutex();
        //And add it to the dest device.
        deviceData.get(dest).enterDevice(comp);
        deviceData.get(dest).acquireFreeMemory();
        //Finally, release mutex and perform the "perform" action.
        transferMUTEX.release();
        transfer.perform();
        //End of transfer. Time to notify everyone that the component is being
        //operated on no more.
        //Time to remove the component from the source device.
        deviceData.get(src).leaveDevice(comp);
        //End of transfer. Time to inform everyone about that.
        acquireTransferMutex();
        componentsStates.put(comp, false);
        //Time to remove the component from the source device.
        deviceData.get(src).leaveDevice(comp);
        transferMUTEX.release();
    }

    private void moveComponentWithFreeMemoryInTheFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        //Add component to the list of components
        //leaving the source device.
        deviceData.get(src).addComponentLeavingDevice(comp);
        //Release mutex, so other processes can perform their executes
        // in a concurrent manner.
        transferMUTEX.release();
        transfer.prepare();
        //Now we want to wait for the free memory slot until it is available.
        deviceData.get(dest).acquireFreeMemory();
        //We finished the preparation phrase, now we need to enter
        //the critical section again, so we can perform necessary
        //transition of the component.
        acquireTransferMutex();
        //And add it to the dest device.
        deviceData.get(dest).enterDevice(comp);
        //Finally, release mutex and perform the "perform" action.
        transferMUTEX.release();
        transfer.perform();
        //End of transfer. Time to inform everyone about that.
        acquireTransferMutex();
        componentsStates.put(comp, false);
        //Time to remove the component from the source device.
        deviceData.get(src).leaveDevice(comp);
        transferMUTEX.release();
    }

    private boolean bfsOnTransfers (DeviceId id){
        Queue<DeviceId> deviceQueue = new ArrayDeque<>();
        for(ComponentTransfer t : deviceData.get(id).getTransfersWaitingForMemory()){
            deviceQueue.add(t.getDestinationDeviceId());
        }
        while(!deviceQueue.isEmpty()){
            DeviceId popped = deviceQueue.poll();
            for(ComponentTransfer t : deviceData.get(popped).getTransfersWaitingForMemory()){
                if(t.getDestinationDeviceId().equals(id)){return true;}
                else{
                    deviceQueue.add(t.getDestinationDeviceId());
                }
            }
        }

        return false;
    }

    //Function  that performs the operation of moving a component from
    //the device A to the device B. It inherits the critical section
    //from the execute() method.
    private void moveComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .getNrOfFreeMemorySlots() > 0){
            moveComponentWithFreeMemorySpace(transfer);
        }
        else {//freeMemorySlots == 0
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .getNrOfComponentsLeavingDevice() > 0){
                moveComponentWithFreeMemoryInTheFuture(transfer);
            }
            else{//No component is leaving the dest device.
                transferMUTEX.release();
                deviceData.get(transfer.getDestinationDeviceId())
                        .acquireFreeMemoryInFuture();
                acquireTransferMutex();
                moveComponentWithFreeMemoryInTheFuture(transfer);
            }
        }
    }

    //Function  that performs the operation of deleting a component
    // from the source device. It inherits the critical section
    //from the execute() method.
    private void deleteComponentOperation(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        //Add component to the list of components
        //leaving the source device.
        deviceData.get(src).addComponentLeavingDevice(comp);
        //Now the first part of the deletion will begin, so before that, we
        //will release the transfer mutex.
        transferMUTEX.release();
        transfer.prepare();
        //It's time to inform everyone, that a memory slot has become
        //available, and the first thing we need to do is to get into the
        //critical section...
        acquireTransferMutex();
        //...and then update all the necessary data.
        deviceData.get(src).leaveDevice(comp);
        //Finally, release mutex and perform the "perform" action.
        transferMUTEX.release();
        transfer.perform();
        //End of transfer. Time to delete the remaining data about the
        //deleted component from the system.
        acquireTransferMutex();
        componentsStates.remove(transfer.getComponentId());
        transferMUTEX.release();
    }

    private void addComponentWithFreeMemory(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        //Update the number of free memory slots in the dest device.
        deviceData.get(dest).decrementFreeSpace();
        //Release mutex, so other processes can perform their executes
        // in a concurrent manner.
        transferMUTEX.release();
        transfer.prepare();
        //We finished the preparation phrase, now we need to enter
        //the critical section again, so we can perform necessary
        //addition of the component.
        acquireTransferMutex();
        //Time to add the comp to the dest device.
        deviceData.get(dest).enterDevice(comp);
        deviceData.get(dest).acquireFreeMemory();
        //Finally, release mutex and perform the "perform" action.
        transferMUTEX.release();
        transfer.perform();
        //End of transfer. Time to notify everyone that the component is being
        //operated on no more.
        setCompStateToFalse(transfer.getComponentId());
    }

    private void addComponentWithFreeMemoryInFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        //Remove transfer from the list of the tranfers waiting for the
        //memory if it is in it. If it was, it probably means that we closed
        //a cycle.
        deviceData.get(src).removeTransferFromWaitingForMemory(transfer);
        //Release mutex, so other processes can perform their executes
        // in a concurrent manner.
        transferMUTEX.release();
        transfer.prepare();
        //Now we want to wait for the free memory slot until it is available.
        deviceData.get(dest).acquireFreeMemory();
        //Here we should have received an access to the freshly
        // freed memory slot.
        acquireTransferMutex();
        //Add the new component to the dest device.
        deviceData.get(dest).enterDevice(comp);
        //Finally, release mutex and perform the "perform" action.
        transferMUTEX.release();
        transfer.perform();
        //End of transfer. Time to inform everyone about that.
        setCompStateToFalse(transfer.getComponentId());
    }

    //Function  that performs the operation of adding a component
    // to the destination device. It inherits the critical section
    //from the execute() method.
    private void addComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .getNrOfFreeMemorySlots() > 0){
            addComponentWithFreeMemory(transfer);
        }
        else {//freeMemorySlots == 0
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .getNrOfComponentsLeavingDevice() > 0){
                addComponentWithFreeMemoryInFuture(transfer);
            }
            else{//No component is leaving the dest device.
                deviceData.get(transfer.getSourceDeviceId())
                                .addTransferWaitingForMemory(transfer);
                if(bfsOnTransfers(transfer.getSourceDeviceId())){
                    addComponentWithFreeMemoryInFuture(transfer);
                }
                else{
                    transferMUTEX.release();
                    deviceData.get(transfer.getDestinationDeviceId())
                            .acquireFreeMemoryInFuture();
                    acquireTransferMutex();
                    addComponentWithFreeMemoryInFuture(transfer);
                }
            }
        }
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        //1.Entering critical section, acquire mutex.
        acquireTransferMutex();
        //2. Validate transfer. If something,s wrong, we RELEASE MUTEX FIRST,
        //and then throw an exception.
        validateTransfer(transfer);
        //3. Everything was fine until now, so it's time to determine what
        //type of operation we are performing.
        if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() != null){
            //We are transferring a component from one device to another.

            //A) It's time to inform everyone that we started the transfer process
            //on the given component.
            componentsStates.put(transfer.getComponentId(), true);
            //B) Let's start the transfer operation.
            moveComponentOperation(transfer);
            //The end, mutex will be released inside the moveComponentOperation() helper functions.
        }
        else if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() == null){
            //We are deleting a component from the source component.

            //A) Inform everyone that we are starting a process.
            componentsStates.put(transfer.getComponentId(), true);
            //B) Let's start the deletion operation.
            deleteComponentOperation(transfer);
            //The end, mutex will be released inside the deleteComponentOperation() function.
        }
        else if(transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() != null){
            //We are adding a new component to the destination device.

            //A) Inform everyone that we are starting a process.
            componentsStates.put(transfer.getComponentId(), true);
            //B) Let's start the adding operation.
            addComponentOperation(transfer);
        }
    }
}
