package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;

import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

//This class holds all data if the device with the given id, as well as
//implements useful functions to process this data.
public class DeviceDataWrapper {
    private int nrOfFreeMemorySlots;
    private int deviceSize;
    private Map<ComponentId, Integer> memoryMapping;
    private ArrayList<ComponentId> componentsInsideDevice;
    private ArrayList<ComponentId> componentsLeavingDevice;
    private ArrayList<ComponentId> componentsWaitingToEnter;
    private ArrayList<Semaphore> memoryCells;
    private Semaphore awaitFreeMemorySpaceInFuture;

    //Functions represent two types of operation: operations on memory that
    //SHOULD BE protected by mutex, and operations on semaphores, which
    //SHOULD NOT BE protected by mutex;

    public DeviceDataWrapper(ArrayList<ComponentId> components, int slots){
        deviceSize = slots;
        componentsInsideDevice = components;
        nrOfFreeMemorySlots = deviceSize - components.size();
        componentsLeavingDevice = new ArrayList<>();
        componentsWaitingToEnter = new ArrayList<>();
        awaitFreeMemorySpaceInFuture = new Semaphore(0, true);
        memoryCells = new ArrayList<>();
        memoryMapping = new HashMap<>();
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

    public void acquireFreeMemoryCell(int i){
        try {
            memoryCells.get(i).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    public void acquireFreeMemoryInFuture(){
        try {
            awaitFreeMemorySpaceInFuture.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    public int getNrOfFreeMemorySlots(){
        return nrOfFreeMemorySlots;
    }

    public void addComponentLeavingDevice(ComponentId comp){
        componentsLeavingDevice.add(comp);
    }

    public void removeComponentFromLeavingDevice(ComponentId comp){
        componentsLeavingDevice.remove(comp);
    }

    public void releaseMemoryCell(ComponentId comp){
        memoryCells.get(memoryMapping.get(comp)).release();
    }

    public void decreaseNrOfFreeMemorySlots(){
        --nrOfFreeMemorySlots;
    }

    public void enterDevice(ComponentId comp){
        componentsInsideDevice.add(comp);
    }

    public void assignFirstFreeMemorySlot(ComponentId comp){
        for(int i = 0; i < memoryCells.size(); ++i){
            if(memoryCells.get(i).availablePermits() == 1){
                acquireFreeMemoryCell(i);
                memoryMapping.put(comp, i);
                break;
            }
        }
    }

    public void leaveDevice(ComponentId comp){
        componentsInsideDevice.remove(comp);
        removeComponentFromLeavingDevice(comp);
        memoryMapping.remove(comp);
    }

    public int getNrOfComponentsLeavingDevice(){
        return componentsLeavingDevice.size();
    }

    public int getMemoryOfTheFirstLeavingComponent(){
        int result = memoryMapping
                .get(componentsLeavingDevice.get(0));
        componentsLeavingDevice.remove(0);
        return result;
    }

    public void addComponentWaitingToEnter(ComponentId comp){
        componentsWaitingToEnter.add(comp);
    }
}
