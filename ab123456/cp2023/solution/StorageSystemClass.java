package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.*;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

//TODO: Add while loops around every await();

public class StorageSystemClass implements StorageSystem {
    //Map containing data structure with data for every device.
    Map<DeviceId, DeviceDataWrapper> deviceData;
    //Map used to keep track of the ongoing transfers on each component.
    private Map<ComponentId, Boolean> componentsStates;
    private ReentrantLock transferMUTEXLock = new ReentrantLock(true);
    private Condition noMemoryLockCondition = transferMUTEXLock.newCondition();
    private Semaphore test = new Semaphore(0, true);

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

    public void awaitOnNoMemoryLockCondition(){
        try {
            noMemoryLockCondition.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
        finally{
            transferMUTEXLock.unlock();
        }
    }

    //Validates transfer, and if there's anything wrong, this function
    //released transferMUTEXLock and throws a corresponding exception.
    private void validateTransfer(ComponentTransfer transfer) throws TransferException {
        if(componentsStates.containsKey(transfer.getComponentId()) &&
                componentsStates.get(transfer.getComponentId())){
            //There's already an ongoing transfer on the given component.
            transferMUTEXLock.unlock();
            throw new ComponentIsBeingOperatedOn(transfer.getComponentId());
        }
        else if(transfer.getSourceDeviceId() != null &&
                !deviceData.containsKey(transfer.getSourceDeviceId())){
            //Source device does not exist.
            transferMUTEXLock.unlock();
            throw new DeviceDoesNotExist(transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                !deviceData.containsKey(transfer.getDestinationDeviceId())){
            //Destination device does not exist.
            transferMUTEXLock.unlock();
            throw new DeviceDoesNotExist(transfer.getDestinationDeviceId());
        }
        else if(transfer.getSourceDeviceId() != null &&
                !deviceData.get(transfer.getSourceDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEXLock.unlock();
            throw new ComponentDoesNotExist(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
        else if(transfer.getDestinationDeviceId() != null &&
                deviceData.get(transfer.getDestinationDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEXLock.unlock();
            throw new ComponentDoesNotNeedTransfer(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
    }

    private void moveComponentWithFreeMemorySpace(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        deviceData.get(dest).decreaseNrOfFreeMemorySlots();
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        deviceData.get(src).removeComponentFromLeavingDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        deviceData.get(dest).assignFirstFreeMemorySlot(comp);
        transferMUTEXLock.unlock();
        deviceData.get(src).releaseMemoryCell(comp);
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        deviceData.get(src).leaveDevice(comp);
        transferMUTEXLock.unlock();
    }

    private void moveComponentWithFreeMemoryInTheFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        int memoryIndex = deviceData.get(dest)
                .getMemoryOfTheFirstLeavingComponent();
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireFreeMemoryCell(memoryIndex);
        transferMUTEXLock.lock();
        deviceData.get(src).removeComponentFromLeavingDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        deviceData.get(src).releaseMemoryCell(comp);
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        deviceData.get(src).leaveDevice(comp);
        transferMUTEXLock.unlock();
    }

    private void moveComponentWithFreeMemoryInTheFutureSecond(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        int memoryIndex = deviceData.get(dest)
                .getMemoryOfTheFirstLeavingComponent();
        if(transferMUTEXLock.hasWaiters(noMemoryLockCondition)){
            noMemoryLockCondition.signal();
            try {
                test.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        transfer.prepare();

        deviceData.get(dest).acquireFreeMemoryCell(memoryIndex);
        transferMUTEXLock.lock();
        deviceData.get(src).removeComponentFromLeavingDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        deviceData.get(src).releaseMemoryCell(comp);
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        deviceData.get(src).leaveDevice(comp);
        transferMUTEXLock.unlock();
    }

    /*private boolean bfsOnTransfers (DeviceId id){
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
    }*/

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
                awaitOnNoMemoryLockCondition();
                //transferMUTEXLock.lock();
                moveComponentWithFreeMemoryInTheFutureSecond(transfer);
            }
        }
    }

    //Function  that performs the operation of deleting a component
    // from the source device. It inherits the critical section
    //from the execute() method.
    private void deleteComponentOperation(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        deviceData.get(src).addComponentLeavingDevice(comp);
        if(transferMUTEXLock.hasWaiters(noMemoryLockCondition)){
            noMemoryLockCondition.signal();
            try {
                test.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        boolean bWasTheMemoryUsed = deviceData.get(src).removeComponentFromLeavingDevice(comp);
        if(!bWasTheMemoryUsed){
            deviceData.get(src).releaseMemoryCell(comp);
        }
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        deviceData.get(src).leaveDevice(comp);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithFreeMemory(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).decreaseNrOfFreeMemorySlots();
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        deviceData.get(dest).assignFirstFreeMemorySlot(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithFreeMemoryInFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        int memoryIndex = deviceData.get(dest)
                .getMemoryOfTheFirstLeavingComponent();
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireFreeMemoryCell(memoryIndex);
        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithFreeMemoryInFutureSecond(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        int memoryIndex = deviceData.get(dest)
                .getMemoryOfTheFirstLeavingComponent();
        test.release();
        transfer.prepare();

        deviceData.get(dest).acquireFreeMemoryCell(memoryIndex);
        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
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
                awaitOnNoMemoryLockCondition();
                //transferMUTEXLock.lock();
                addComponentWithFreeMemoryInFutureSecond(transfer);
            }
        }
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        //1.Entering critical section, acquire mutex.
        transferMUTEXLock.lock();
        //2. Validate transfer. If something's wrong, we RELEASE MUTEX FIRST,
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
