package cp2023.solution;
import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
//This class holds all data if the device with the given id, as well as
//implements useful functions to process this data.
public class DeviceDataWrapper {
    private final int deviceSize;
    private Map<ComponentId, Integer> memoryMapping;
    private Map<ComponentId, Integer> reservedMemory;
    private ArrayList<ComponentId> componentsInsideDevice;
    private ArrayList<ComponentId> componentsLeavingDevice;
    private ArrayList<Semaphore> memoryCells;
    public ArrayList<ComponentTransfer> waitingTransfers = new ArrayList<>();

    //Constructor.
    public DeviceDataWrapper(ArrayList<ComponentId> components, int slots){
        deviceSize = slots;
        componentsInsideDevice = components;
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

    //Acquires the memory cell with the given id.
    public void acquireMemoryCell(int id){
        try {
            memoryCells.get(id).acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }

    boolean isComponentInDevice(ComponentId comp){
        return componentsInsideDevice.contains(comp);
    }

    //Adds the given component to the list of components leaving device.
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

    //Add the given device to the list of components inside this device.
    public void enterDevice(ComponentId comp){
        componentsInsideDevice.add(comp);
    }

    //Looks for the first memory cell that has no memory mapping in either
    //memoryMapping or reservedMemory maps, acquires it and stores new
    //information about the memory mapping in the memoryMapping HashMap.
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

    //Acquires a memory cell with the id mapped to the given component
    //in reservedMemory map. The assumption is that this function is used only
    //When reserveMemorySpace() function was called before for the given comp.
    public void acquireReservedMemory(ComponentId comp){
        int index = reservedMemory.get(comp);
        memoryMapping.put(comp, index);
        reservedMemory.remove(comp);
        acquireMemoryCell(index);
    }

    //Removes the given comp from this device data structures.
    public void leaveDevice(ComponentId comp){
        componentsInsideDevice.remove(comp);
        componentsLeavingDevice.remove(comp);
        memoryMapping.remove(comp);
    }

    //Reserves the memory slot of the first component that is marked as the one
    //leaving the device (is contained inside componentsLeavingDevice list).
    //The assumption is that this function will be used only when we know that
    //there is no free memory present, but there definitely will, because
    //there is at least one component leaving the device.
    public void reserveMemorySpace(ComponentId comp){
        int result = memoryMapping
                .get(componentsLeavingDevice.get(0));
        componentsLeavingDevice.remove(0);
        reservedMemory.put(comp, result);
    }
}