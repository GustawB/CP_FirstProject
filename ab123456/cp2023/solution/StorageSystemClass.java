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
    private Map<DeviceId, Condition> conditionsForEveryDevice;
    private Condition noMemoryLockCondition = transferMUTEXLock.newCondition();
    private Semaphore inheritCS = new Semaphore(0, true);

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
        conditionsForEveryDevice = new HashMap<>();
        for(Map.Entry<DeviceId, Integer> entry : deviceTotalSlots.entrySet()){
            if(componentsInDevice.containsKey(entry.getKey())){
                deviceData.put(entry.getKey(), new DeviceDataWrapper(
                        componentsInDevice.get(entry.getKey()), entry.getValue()));
            }
            else{
                deviceData.put(entry.getKey(), new DeviceDataWrapper(
                        new ArrayList<>(), entry.getValue()));
            }
            conditionsForEveryDevice.put(entry.getKey(), transferMUTEXLock.newCondition());
        }
        //initialize mutexex for each component;
        componentsStates = new HashMap<>();
        for(Map.Entry<ComponentId, DeviceId> entry : componentPlacement.entrySet()){
            componentsStates.put(entry.getKey(), false);
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

    public void ugabugaSex(ComponentTransfer transfer){
        DeviceId src = transfer.getSourceDeviceId();
        if(transferMUTEXLock.hasWaiters(conditionsForEveryDevice.get(src))){
            conditionsForEveryDevice.get(src).signal();
            try {
                inheritCS.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void moveComponentWithFreeMemorySpace(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        deviceData.get(dest).acquireFirstFreeMemorySlot(comp);
        ugabugaSex(transfer);
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(dest).enterDevice(comp);
        deviceData.get(src).releaseMemoryCell(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void moveComponentWithFreeMemoryInTheFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        deviceData.get(dest).reserveMemorySpace(comp);
        ugabugaSex(transfer);
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireReservedMemory(comp);
        transferMUTEXLock.lock();
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(src).releaseMemoryCell(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void moveComponentWithInheritedCS(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(src).addComponentLeavingDevice(comp);
        deviceData.get(dest).reserveMemorySpace(comp);
        inheritCS.release();
        transferMUTEXLock.lock();//Synchronize all
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireReservedMemory(comp);
        transferMUTEXLock.lock();
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(src).releaseMemoryCell(comp);
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void moveComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .hasFreeMemorySpace()){
            moveComponentWithFreeMemorySpace(transfer);
        }
        else {//freeMemorySlots == 0
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .willHaveFreeMemorySpace()){
                moveComponentWithFreeMemoryInTheFuture(transfer);
            }
            else{//No component is leaving the dest device.
                try {
                    while (!deviceData.get(transfer.getDestinationDeviceId())
                            .hasFreeMemorySpace() &&
                            !deviceData.get(transfer.getDestinationDeviceId())
                                    .willHaveFreeMemorySpace()) {
                        System.out.println("move inside while-loop");
                        conditionsForEveryDevice.get(transfer.getDestinationDeviceId()).await();
                    }
                } catch (InterruptedException e) {
                    transferMUTEXLock.unlock();
                    throw new RuntimeException(e);
                }
                System.out.println("move outside while-loop");
                moveComponentWithInheritedCS(transfer);
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
        ugabugaSex(transfer);
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        deviceData.get(src).leaveDevice(comp);
        deviceData.get(src).releaseMemoryCell(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithFreeMemory(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).acquireFirstFreeMemorySlot(comp);
        transferMUTEXLock.unlock();
        transfer.prepare();

        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithFreeMemoryInFuture(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).reserveMemorySpace(comp);
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireReservedMemory(comp);
        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentWithInheritedCS(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId dest = transfer.getDestinationDeviceId();

        deviceData.get(dest).reserveMemorySpace(comp);
        inheritCS.release();
        transferMUTEXLock.lock();//Synchronize all
        transferMUTEXLock.unlock();
        transfer.prepare();

        deviceData.get(dest).acquireReservedMemory(comp);
        transferMUTEXLock.lock();
        deviceData.get(dest).enterDevice(comp);
        transferMUTEXLock.unlock();
        transfer.perform();

        transferMUTEXLock.lock();
        componentsStates.put(comp, false);
        transferMUTEXLock.unlock();
    }

    private void addComponentOperation(ComponentTransfer transfer){
        if(deviceData.get(transfer.getDestinationDeviceId())
                .hasFreeMemorySpace()){
            addComponentWithFreeMemory(transfer);
        }
        else {//freeMemorySlots == 0
            if(deviceData.get(transfer.getDestinationDeviceId())
                    .willHaveFreeMemorySpace()){
                addComponentWithFreeMemoryInFuture(transfer);
            }
            else{//No component is leaving the dest device.
                try {
                    while (!deviceData.get(transfer.getDestinationDeviceId())
                            .hasFreeMemorySpace() &&
                            !deviceData.get(transfer.getDestinationDeviceId())
                                    .willHaveFreeMemorySpace()) {
                        System.out.println("add inside while-loop");
                        conditionsForEveryDevice.get(transfer.getDestinationDeviceId()).await();
                    }
                } catch (InterruptedException e) {
                    transferMUTEXLock.unlock();
                    throw new RuntimeException(e);
                }

                System.out.println("add outside while-loop");
                addComponentWithInheritedCS(transfer);
            }
        }
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        transferMUTEXLock.lock();
        validateTransfer(transfer);
        if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() != null){
            componentsStates.put(transfer.getComponentId(), true);
            moveComponentOperation(transfer);
        }
        else if(transfer.getSourceDeviceId() != null && transfer.getDestinationDeviceId() == null){
            componentsStates.put(transfer.getComponentId(), true);
            deleteComponentOperation(transfer);
        }
        else if(transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() != null){
            componentsStates.put(transfer.getComponentId(), true);
            addComponentOperation(transfer);
        }
    }
}