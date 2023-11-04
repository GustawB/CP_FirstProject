package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

//This class holds all data if the device with the given id, as well as
//implements useful functions to process this data.
public class DeviceDataWrapper {
    private int nrOfFreeMemorySlots;
    private int nrOfComponentsLeavingDevice;
    private int deviceSize;
    private ArrayList<ComponentId> componentsInsideDevice;
    private ArrayList<ComponentId> componentsLeavingDevice;
    private ArrayList<ComponentTransfer> transfersWaitingForMemory;
    private Semaphore freeMemorySpace;
    private Semaphore freeMemorySpaceInFuture;

    public DeviceDataWrapper(ArrayList<ComponentId> components, int slots){
        deviceSize = slots;
        componentsInsideDevice = components;
        nrOfFreeMemorySlots = deviceSize - components.size();
        componentsLeavingDevice = new ArrayList<>();
        transfersWaitingForMemory = new ArrayList<>();
        nrOfComponentsLeavingDevice = 0;
        freeMemorySpace = new Semaphore(nrOfFreeMemorySlots, true);
        freeMemorySpaceInFuture = new Semaphore(0, true);
    }

    public void acquireFreeMemory(){
        try {
            freeMemorySpace.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    public void acquireFreeMemoryInFuture(){
        try {
            freeMemorySpaceInFuture.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    public boolean isComponentInDevice(ComponentId componentId){
        return componentsInsideDevice.contains(componentId);
    }

    public int getNrOfFreeMemorySlots(){
        return nrOfFreeMemorySlots;
    }

    public int getNrOfComponentsLeavingDevice(){
        return nrOfComponentsLeavingDevice;
    }

    public ArrayList<ComponentTransfer> getTransfersWaitingForMemory(){
        return transfersWaitingForMemory;
    }

    public void addTransferWaitingForMemory(ComponentTransfer transfer){
        transfersWaitingForMemory.add(transfer);
    }

    public void removeTransferFromWaitingForMemory(ComponentTransfer transfer){
        transfersWaitingForMemory.remove(transfer);
    }

    public void addComponentLeavingDevice(ComponentId comp){
        componentsLeavingDevice.add(comp);
        ++nrOfComponentsLeavingDevice;
        freeMemorySpaceInFuture.release();
    }

    public void leaveDevice(ComponentId comp){
        componentsLeavingDevice.remove(comp);
        componentsInsideDevice.remove(comp);
        acquireFreeMemoryInFuture();
        freeMemorySpace.release();
        ++nrOfFreeMemorySlots;
        --nrOfComponentsLeavingDevice;
    }

    public void enterDevice(ComponentId comp){
        componentsInsideDevice.add(comp);
        --nrOfFreeMemorySlots;
    }

    public void decrementFreeSpace(){
        --nrOfFreeMemorySlots;
        acquireFreeMemory();
    }
}
