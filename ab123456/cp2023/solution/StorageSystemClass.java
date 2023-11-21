package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.*;

import java.util.*;
import java.util.concurrent.Semaphore;

public class StorageSystemClass implements StorageSystem {
    Map<DeviceId, DeviceDataWrapper> deviceData;
    private Map<ComponentId, Boolean> componentsStates;
    private Semaphore transferMutex = new Semaphore(1, true);
    private ArrayList<ComponentTransfer> transfers = new ArrayList<>();
    private Map<ComponentTransfer, Semaphore>
            transfersSemaphores = new HashMap<>();
    private ArrayList<ComponentTransfer>
            memoryTriggers = new ArrayList<>();
    private HashMap<ComponentTransfer, Semaphore>
            memoryTriggersMapping = new HashMap<>();
    private boolean bWasThereACycle = false;
    private ArrayList<ComponentTransfer>
            cycleTransfersListSaver = new ArrayList<>();


    public StorageSystemClass(Map<DeviceId, Integer> deviceTotalSlots,
                              Map<ComponentId, DeviceId> componentPlacement) {
        //Check whether there is a component assigned to the device without
        //defined size, or if there are too many components assigned
        //to one device.
        if(componentPlacement == null){
            throw new IllegalArgumentException
                    ("Null passed as componentPlacement");
        }
        Map<DeviceId, ArrayList<ComponentId>> componentsInDevice
                = new HashMap<>();
        for (Map.Entry<ComponentId, DeviceId> me :
                componentPlacement.entrySet()) {
            if (!deviceTotalSlots.containsKey(me.getValue())) {
                throw new IllegalArgumentException(
                        "Component assigned to the device " +
                                "without specified size.");
            }
            if(!componentsInDevice.containsKey(me.getValue())){
                componentsInDevice.put(me.getValue(), new ArrayList<>());
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
                        componentsInDevice.get(entry.getKey()),
                        entry.getValue()));
            }
            else{
                deviceData.put(entry.getKey(), new DeviceDataWrapper(
                        new ArrayList<>(), entry.getValue()));
            }
        }
        //initialize mutexex for each component;
        componentsStates = new HashMap<>();
        for(Map.Entry<ComponentId, DeviceId> entry :
                componentPlacement.entrySet()){
            componentsStates.put(entry.getKey(), false);
        }
    }

    //Validates transfer, and if there's anything wrong, this function
    //released transferMUTEXLock and throws a corresponding exception.
    private void validateTransfer(ComponentTransfer transfer)
            throws TransferException {
        if(componentsStates.containsKey(transfer.getComponentId())){
            if(transfer.getSourceDeviceId() == null &&
                    transfer.getDestinationDeviceId() != null &&
                    deviceData.get(transfer.getDestinationDeviceId())
                            .isComponentInDevice(transfer.getComponentId())){
                //We are trying to add something that already exist.
                transferMutex.release();
                throw new ComponentAlreadyExists(transfer.getComponentId(),
                        transfer.getDestinationDeviceId());
            }
            else if(componentsStates.get(transfer.getComponentId())) {
                //We are requesting an operation on something that is
                //already being operated on.
                transferMutex.release();
                throw new ComponentIsBeingOperatedOn(transfer.getComponentId());
            }
        }

        if(transfer.getSourceDeviceId() != null &&
                !deviceData.containsKey(transfer.getSourceDeviceId())){
            //Source device does not exist.
            transferMutex.release();
            throw new DeviceDoesNotExist(transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                !deviceData.containsKey(transfer.getDestinationDeviceId())){
            //Destination device does not exist.
            transferMutex.release();
            throw new DeviceDoesNotExist(transfer.getDestinationDeviceId());
        }
        else if(transfer.getSourceDeviceId() != null &&
                !deviceData.get(transfer.getSourceDeviceId())
                        .isComponentInDevice(transfer.getComponentId())){
            transferMutex.release();
            throw new ComponentDoesNotExist(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                deviceData.get(transfer.getDestinationDeviceId())
                        .isComponentInDevice(transfer.getComponentId())){
            transferMutex.release();
            throw new ComponentDoesNotNeedTransfer(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
    }

    private void acquireTransferMutex(){
        try {
            transferMutex.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void acquireTransfersSemaphore(ComponentTransfer t){
        try {
            transfersSemaphores.get(t).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void acquireTriggersSemaphore(ComponentTransfer t){
        try {
            memoryTriggersMapping.get(t).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private int findIndexOfFirstWaiter(ComponentTransfer transfer){
        for(int i = 0; i < transfers.size(); ++i){
            if(transfers.get(i).getDestinationDeviceId()
                    .equals(transfer.getSourceDeviceId())){
                return i;
            }
        }
        return -1;
    }

    private void waitForPreviousWaiter(ComponentTransfer transfer, int index){
        if(index >= 0){
            memoryTriggers.add(transfer);
            memoryTriggersMapping.put(transfer, new Semaphore(0, true));
            transfersSemaphores.get(transfers.get(index)).release();
            acquireTriggersSemaphore(transfer);
            memoryTriggersMapping.remove(transfer);
            memoryTriggers.remove(transfer);
        }
    }

    private void removeWaiter(ComponentTransfer transfer){
        transfers.remove(transfer);
        transfersSemaphores.remove(transfer);
    }

    private boolean hasCycle(ComponentTransfer transfer,
                             ArrayList<ComponentTransfer> waitingTransfers,
                             ArrayList<ComponentTransfer> result,
                             HashSet<DeviceId> visitedDevices){
        //We found the cycle.
        if(result.size() > 0 && result.get(result.size()-1) == transfer){return true;}
        //We visited a device that we know that have no waiting transfer that
        //will lead to the cycle.
        else if(visitedDevices.contains(transfer.getSourceDeviceId())){return false;}
        //Recursive DFS.
        for (ComponentTransfer waitingTransfer : waitingTransfers) {
            result.add(waitingTransfer);
            if (hasCycle(transfer,
                    deviceData.get(waitingTransfer.getSourceDeviceId())
                            .waitingTransfers, result, visitedDevices)) {
                return true;
            }
            result.remove(waitingTransfer);
        }
        if(waitingTransfers.size() > 0){
            //If the destination device has waiting transfers, and we checked
            //that none of it leads to the cycle, we add it to the
            //visitedDevices HashSet so that we won't perform DFS on it again.
            visitedDevices.add(waitingTransfers.get(0).getDestinationDeviceId());
        }

        return false;
    }

    private void moveComponentWithFreeMemorySpace(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        deviceData.get(dest).acquireFirstFreeMemorySlot(comp);
        //If there is someone waiting for the memory, we notify him
        //that he can resume his operations, and we wait for him
        //to safely update all necessary information.
        int index = findIndexOfFirstWaiter(transfer);
        waitForPreviousWaiter(transfer, index);
        //We did everything we wanted, release mutex and prepare().
        transferMutex.release();
        transfer.prepare();

        //Free-up memory space.
        deviceData.get(src).releaseMemoryCell(comp);
        acquireTransferMutex();
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void moveComponentWithFreeMemoryInTheFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        //Reserve the memory slot owned by on of the components leaving device.
        deviceData.get(dest).reserveMemorySpace(comp);
        //If there is someone waiting for the memory, we notify him
        //that he can resume his operations, and we wait for him
        //to safely update all necessary information.
        int index = findIndexOfFirstWaiter(transfer);
        waitForPreviousWaiter(transfer, index);
        transferMutex.release();
        transfer.prepare();

        deviceData.get(src).releaseMemoryCell(comp);
        acquireTransferMutex();
        //Acquire memory reserved before.
        deviceData.get(dest).acquireReservedMemory(comp);
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void moveComponentWithInheritedCS(ComponentTransfer transfer,
                                              boolean bWasClosingCycle){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        //Remove transfer from the list of transfers awaiting to enter
        //a given device.
        deviceData.get(transfer.getDestinationDeviceId())
                .waitingTransfers.remove(transfer);
        if(!bWasClosingCycle){
            deviceData.get(dest).reserveMemorySpace(comp);
        }
        //Remove transfer from the list of transfers waiting for any memory.
        removeWaiter(transfer);
        if(bWasThereACycle && transfers.isEmpty()){
            //We processed every transfer that was part of the cycle,
            //restore previous information.
            bWasThereACycle = false;
            transfers.addAll(cycleTransfersListSaver);
            cycleTransfersListSaver = new ArrayList<>();
        }
        else{
            //If there is someone waiting for the memory, we notify him
            //that he can resume his operations, and we wait for him
            //to safely update all necessary information.
            int index = findIndexOfFirstWaiter(transfer);
            waitForPreviousWaiter(transfer, index);
        }
        if(!bWasClosingCycle) {
            //Resume operations of the transfer that woke us up.
            memoryTriggersMapping.get(memoryTriggers
                    .get(memoryTriggers.size() - 1)).release();
        }
        else{
            //This transfer is closing a cycle, so it can reserve memory
            //space only if all other transfers in the cycle
            // performed their actions.
            deviceData.get(dest).reserveMemorySpace(comp);
            transferMutex.release();
        }
        transfer.prepare();

        deviceData.get(src).releaseMemoryCell(comp);
        acquireTransferMutex();
        deviceData.get(dest).acquireReservedMemory(comp);
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void moveComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .hasFreeMemorySpace()){
            //We have a free memory space, so we proceed.
            moveComponentWithFreeMemorySpace(transfer);
        }
        else {//freeMemorySlots == 0
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .willHaveFreeMemorySpace()){
                //We know that there is at least one component leaving
                //the dest device, so we proceed.
                moveComponentWithFreeMemoryInTheFuture(transfer);
            }
            else{//No component is leaving the dest device.
                //Add transfer to data structures holding informaton about
                //waiting transfers.
                transfers.add(transfer);
                transfersSemaphores.put(transfer,
                        new Semaphore(0,true));
                ArrayList<ComponentTransfer> cycle = new ArrayList<>();
                deviceData.get(transfer.getDestinationDeviceId())
                        .waitingTransfers.add(transfer);
                if(hasCycle(transfer,
                        deviceData.get(transfer.getSourceDeviceId())
                                .waitingTransfers, cycle, new HashSet<>())){
                    //We found the cycle, we need to store the information
                    //about the waiting transfers that aren't in the cycle,
                    //and move the cycle into the transfers list so that
                    //the cycle will be resolved.
                    for(ComponentTransfer t : cycle){
                        transfers.remove(t);
                    }
                    bWasThereACycle = true;
                    cycleTransfersListSaver.addAll(transfers);
                    transfers.clear();
                    transfers.addAll(cycle);
                    moveComponentWithInheritedCS(transfer, true);
                }
                else{
                    //Release mutex and await for someone to wake us up.
                    transferMutex.release();
                    acquireTransfersSemaphore(transfer);

                    //Resume operation, called in another transfer's
                    //critical section.
                    moveComponentWithInheritedCS(transfer, false);
                }
            }
        }
    }

    private void deleteComponentOperation(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        //If there is someone waiting for the memory, we notify him
        //that he can resume his operations, and we wait for him
        //to safely update all necessary information.
        int index = findIndexOfFirstWaiter(transfer);
        waitForPreviousWaiter(transfer, index);
        transferMutex.release();
        transfer.prepare();

        deviceData.get(src).releaseMemoryCell(comp);
        acquireTransferMutex();
        deviceData.get(src).leaveDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void addComponentWithFreeMemory(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).acquireFirstFreeMemorySlot(comp);
        transferMutex.release();
        transfer.prepare();

        acquireTransferMutex();
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void addComponentWithFreeMemoryInFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).reserveMemorySpace(comp);
        transferMutex.release();
        transfer.prepare();

        acquireTransferMutex();
        deviceData.get(dest).acquireReservedMemory(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void addComponentWithInheritedCS(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).reserveMemorySpace(comp);
        removeWaiter(transfer);
        //Wake up the transfer that woke us up.
        memoryTriggersMapping.get(memoryTriggers
                .get(memoryTriggers.size()-1)).release();
        transfer.prepare();

        acquireTransferMutex();
        deviceData.get(dest).acquireReservedMemory(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMutex.release();
        transfer.perform();

        acquireTransferMutex();
        componentsStates.put(comp, false);
        transferMutex.release();
    }

    private void addComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .hasFreeMemorySpace()){
            //There are free memory slots.
            addComponentWithFreeMemory(transfer);
        }
        else {
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .willHaveFreeMemorySpace()){
                //There will be free memory slot.
                addComponentWithFreeMemoryInFuture(transfer);
            }
            else{//No component is leaving the dest device.
                transfers.add(transfer);
                transfersSemaphores.put(transfer,
                        new Semaphore(0,true));
                transferMutex.release();
                //Release mutex and wait for the free memory.
                acquireTransfersSemaphore(transfer);

                //Someone woke us up, we continue operations
                //in his critical section.
                addComponentWithInheritedCS(transfer);
            }
        }
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        acquireTransferMutex(); //Enter the critical section.
        validateTransfer(transfer); //Validate the data.
        //Mark the transferred component as the one that is being operated on.
        componentsStates.put(transfer.getComponentId(), true);
        if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() != null){
            //Move operation case (we have source and destination).
            moveComponentOperation(transfer);
        }
        else if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() == null){
            //Delete operation case (we have source but no destination).
            deleteComponentOperation(transfer);
        }
        else if(transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() != null){
            //Add operation case (we have destination but no source).
            addComponentOperation(transfer);
        }
        else{
            //We have no source and destination, so it is an invalid transfer.
            componentsStates.put(transfer.getComponentId(), false);
            transferMutex.release();
            throw new IllegalTransferType(transfer.getComponentId());
        }
    }
}