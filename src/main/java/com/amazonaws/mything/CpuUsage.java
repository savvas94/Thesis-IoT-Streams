package com.amazonaws.mything;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CpuUsage {
	
    private final static ObjectMapper JSON = new ObjectMapper();
	static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    private long 	timestamp;
    private double 	guest_nice,
				    guest,
				    steal,
				    softirq,
				    irq,
				    user,
				    system,
				    nice,
				    iowait;
    
    public CpuUsage() {
		super();
	}
    
	public CpuUsage(long timestamp, double guest_nice, double guest, double steal, double softirq, double irq,
			double user, double system, double nice, double iowait) {
		super();
		this.timestamp = timestamp;
		this.guest_nice = guest_nice;
		this.guest = guest;
		this.steal = steal;
		this.softirq = softirq;
		this.irq = irq;
		this.user = user;
		this.system = system;
		this.nice = nice;
		this.iowait = iowait;
	}
	
    public String toJsonAsString() {
        try {
			return JSON.writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static CpuUsage fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, CpuUsage.class);
        } catch (IOException e) {
            return null;
        }
    }
    
	@Override
	public String toString() {
		String toReturn;
		if (timestamp == 0) {
			toReturn = "[guest_nice=" + guest_nice + ", guest=" + guest + ", steal="
					+ steal + ", softirq=" + softirq + ", irq=" + irq + ", user=" + user + ", system=" + system + ", nice="
					+ nice + ", iowait=" + iowait + "]";
		}
		else {
			toReturn = "CpuUsage [timestamp=" + timestamp + ", guest_nice=" + guest_nice + ", guest=" + guest + ", steal="
					+ steal + ", softirq=" + softirq + ", irq=" + irq + ", user=" + user + ", system=" + system + ", nice="
					+ nice + ", iowait=" + iowait + "]";
		}
		return toReturn;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public double getGuest_nice() {
		return guest_nice;
	}

	public double getGuest() {
		return guest;
	}

	public double getSteal() {
		return steal;
	}

	public double getSoftirq() {
		return softirq;
	}

	public double getIrq() {
		return irq;
	}

	public double getUser() {
		return user;
	}

	public double getSystem() {
		return system;
	}

	public double getNice() {
		return nice;
	}

	public double getIowait() {
		return iowait;
	}

	public void setGuest_nice(double guest_nice) {
		this.guest_nice = guest_nice;
	}

	public void setGuest(double guest) {
		this.guest = guest;
	}

	public void setSteal(double steal) {
		this.steal = steal;
	}

	public void setSoftirq(double softirq) {
		this.softirq = softirq;
	}

	public void setIrq(double irq) {
		this.irq = irq;
	}

	public void setUser(double user) {
		this.user = user;
	}

	public void setSystem(double system) {
		this.system = system;
	}

	public void setNice(double nice) {
		this.nice = nice;
	}

	public void setIowait(double iowait) {
		this.iowait = iowait;
	}
	
	
    
}
