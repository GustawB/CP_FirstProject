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

    //Validates transfer, and if there's anythong wrong, this functionn
    //released transferMutex and throws a corresponding exception.
    private void validateTransfer(ComponentTransfer transfer) throws TransferException {
        if(componentsStates.containsKey(transfer.getComponentId()) &&
                componentsStates.get(transfer.getComponentId()) == true){
            //There's already an ongoing transfer on the given component.
            transferMUTEX.release();
            throw new ComponentIsBeingOperatedOn(transfer.getComponentId());
        }
        else if(!deviceData.containsKey(transfer.getSourceDeviceId())){
            transferMUTEX.release();
            throw new DeviceDoesNotExist(transfer.getSourceDeviceId());
        }
        else if(!deviceData.containsKey(transfer.getDestinationDeviceId())){
            transferMUTEX.release();
            throw new DeviceDoesNotExist(transfer.getDestinationDeviceId());
        }
        else if(!deviceData.get(transfer.getSourceDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEX.release();
            throw new ComponentDoesNotExist(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
        else if(deviceData.get(transfer.getDestinationDeviceId())
                .isComponentInDevice(transfer.getComponentId())){
            transferMUTEX.release();
            throw new ComponentDoesNotNeedTransfer(transfer.getComponentId(),
                    transfer.getSourceDeviceId());
        }
    }

    //Function  that performs the operation of moving a component from
    //the device A to the device B. It inherits the critical section
    //from the execute() method.
    private void performFromToOperation(ComponentTransfer transfer){
        ComponentId comp = transfer.getComponentId();
        DeviceId src = transfer.getSourceDeviceId();
        DeviceId dest = transfer.getDestinationDeviceId();
        if(deviceData.get(dest).getNrOfFreeMemorySlots() > 0){

        }
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        //1.Entering critical section, acquire mutex.
        try {
            transferMUTEX.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
        //2. Validate transfer. If something,s wrong, we RELEASE MUTEX FIRST,
        //and then throw an exception.
        validateTransfer(transfer);
        //3. Everything was fine until now, so it's time to determine what
        //type of operation we are performing.
        if(transfer.getSourceDeviceId()  != null && transfer.getDestinationDeviceId() != null){
            //We are transfering component from one device to another.

            //A) It's time to inform everyone that we started the transfer process
            //on the given component.
            componentsStates.put(transfer.getComponentId(), true);

            //B)
        }
    }
}
