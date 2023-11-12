package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

//This class holds all data if the device with the given id, as well as
//implements useful functions to process this data.
public class DeviceDataWrapper {
    private int nrOfFreeMemorySlots;
    private final int deviceSize;
    private Map<ComponentId, Integer> memoryMapping;
    private Map<ComponentId, Integer> reservedMemory;
    private ArrayList<ComponentId> componentsInsideDevice;
    private ArrayList<ComponentId> componentsLeavingDevice;
    private ArrayList<Semaphore> memoryCells;
    public ArrayList<ComponentTransfer> waitingTransfers = new ArrayList<>();

    //Functions represent two types of operation: operations on memory that
    //SHOULD BE protected by mutex, and operations on semaphores, which
    //SHOULD NOT BE protected by mutex;

    public DeviceDataWrapper(ArrayList<ComponentId> components, int slots){
        deviceSize = slots;
        componentsInsideDevice = components;
        nrOfFreeMemorySlots = deviceSize - components.size();
        componentsLeavingDevice = new ArrayList<>();
        memoryCells = new ArrayList<>();
        memoryMapping = new HashMap<>();
        reservedMemory = new HashMap<>();
        for(int i = 0; i < deviceSize; ++i){
            if(i < componentsInsideDevice.size()){
                memoryMapping.put(componentsInsideDevice.get(i), i);
                memoryCells.add(new Semaphore(0, true));
            }
            else{
                memoryCells.add(new Semaphore(1, true));
            }
        }
    }

    public void acquireMemoryCell(int i){
        try {
            memoryCells.get(i).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    boolean isComponentInDevice(ComponentId comp){
        return componentsInsideDevice.contains(comp);
    }

    public void addComponentLeavingDevice(ComponentId comp){
        componentsLeavingDevice.add(comp);
    }

    public void releaseMemoryCell(ComponentId comp){
        memoryCells.get(memoryMapping.get(comp)).release();
    }

    public boolean hasFreeMemorySpace(){
        return deviceSize - memoryMapping.size() > 0;
    }

    public boolean willHaveFreeMemorySpace(){
        return componentsLeavingDevice.size() > 0;
    }

    public void enterDevice(ComponentId comp){
        componentsInsideDevice.add(comp);
    }

    public void acquireFirstFreeMemorySlot(ComponentId comp){
        for(int i = 0; i < memoryCells.size(); ++i){
            if(!memoryMapping.containsValue(i) &&
                    !reservedMemory.containsValue(i)){
                acquireMemoryCell(i);
                memoryMapping.put(comp, i);
                break;
            }
        }
    }

    public void acquireReservedMemory(ComponentId comp){
        int index = reservedMemory.get(comp);
        reservedMemory.remove(comp);
        acquireMemoryCell(index);
    }

    public void leaveDevice(ComponentId comp){
        componentsInsideDevice.remove(comp);
        componentsLeavingDevice.remove(comp);
        memoryMapping.remove(comp);
    }

    public void reserveMemorySpace(ComponentId comp){
        int result = memoryMapping
                .get(componentsLeavingDevice.get(0));
        componentsLeavingDevice.remove(0);
        reservedMemory.put(comp, result);
    }
}