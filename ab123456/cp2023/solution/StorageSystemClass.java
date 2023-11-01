package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.DeviceDoesNotExist;
import cp2023.exceptions.IllegalTransferType;
import cp2023.exceptions.TransferException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class StorageSystemClass implements StorageSystem {
    private Map<DeviceId, Integer> deviceTotalSlots;
    private  Map<ComponentId, DeviceId> componentPlacement;
    private Map<DeviceId, Integer> nrOfComponentsInDevice;
    private Map<DeviceId, ArrayList<ComponentId>> componentsAwaitingToGetOut;
    private Map<DeviceId, ArrayDeque<ComponentId>> componentsAwaitingToGetIn;
    private Map<ComponentId, Semaphore> semaphorsForComponents;
    private Semaphore mutex = new Semaphore(1, true);


    public StorageSystemClass(Map<DeviceId, Integer> deviceTotalSlots,
                              Map<ComponentId, DeviceId> componentPlacement) {
        //Check whether there is a component assigned to the device without
        //defined size, or if there are too many components assigned
        //to one device.
        nrOfComponentsInDevice = new HashMap<>();
        for (Map.Entry<ComponentId, DeviceId> me :
                componentPlacement.entrySet()) {
            if (!deviceTotalSlots.containsKey(me.getValue())) {
                throw new IllegalArgumentException(
                        "Component assigned to the device " +
                                "without specified size.");
            }
            nrOfComponentsInDevice.computeIfAbsent(me.getValue(), x -> 0);
            nrOfComponentsInDevice.put(me.getValue(),
                    nrOfComponentsInDevice.get(me.getValue()) + 1);
            if (nrOfComponentsInDevice.get(me.getValue()) >
                    deviceTotalSlots.get(me.getValue())) {
                throw new IllegalArgumentException(
                        "Too many components assigned to the device" +
                                "with id = " + me.getValue());
            }
        }
        //Passed parameters were valid, we can initialize our object.
        this.deviceTotalSlots = deviceTotalSlots;
        this.componentPlacement = componentPlacement;
        this.componentsAwaitingToGetIn = new HashMap<>();
        this.componentsAwaitingToGetOut = new HashMap<>();
    }
}
