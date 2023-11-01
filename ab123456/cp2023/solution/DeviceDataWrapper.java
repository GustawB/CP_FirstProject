package cp2023.solution;

import cp2023.base.ComponentId;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;

//This class holds all data if the device with the given id, as well as
//implements useful functions to process this data.
public class DeviceDataWrapper {
    private int nrOfFreeMemorySlots;
    private int nrOfComponentsLeavingDevice;
    private int deviceSize;
    private List<ComponentId> componentsInsideDevice;
    private List<ComponentId> componentsLeavingDevice;

    public DeviceDataWrapper(ArrayList<ComponentId> components, int slots){
        deviceSize = slots;
        componentsInsideDevice = components;
        nrOfFreeMemorySlots = deviceSize - components.size();
        componentsLeavingDevice = new ArrayList<>();
        nrOfComponentsLeavingDevice = 0;
    }

    public boolean isComponentInDevice(ComponentId componentId){
        return (componentsInsideDevice.contains(componentId) ||
                componentsLeavingDevice.contains(componentId));
    }

    public int getNrOfFreeMemorySlots(){
        return nrOfFreeMemorySlots;
    }
}
