package ru;

import org.apache.spark.storage.StorageLevel;
import org.pcap4j.core.*;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.PacketListener;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.List;

public class ReceiverFromDiv extends Receiver {

    public static int size = 0;
    public String ip = null;
    public PacketListener listener = new PacketListener() {
        @Override
        public void gotPacket(PcapPacket pcapPacket) {
            size += pcapPacket.getOriginalLength();
        }
    };

    public ReceiverFromDiv() {
        super(StorageLevel.MEMORY_ONLY());
    }

    public ReceiverFromDiv(String ip) {
        super(StorageLevel.MEMORY_ONLY());
        this.ip = ip;
    }

    public void getPacketByIP() {
        try {
            InetAddress addr = InetAddress.getByName(ip);
            PcapNetworkInterface nif = Pcaps.getDevByAddress(addr);
            int snapLen = 65536;
            PcapNetworkInterface.PromiscuousMode mode = PcapNetworkInterface.PromiscuousMode.PROMISCUOUS;
            int timeout = 0;
            PcapHandle handle = nif.openLive(snapLen, mode, timeout);
            handle.loop(0, listener);
            handle.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void getAllPackets() {
        int summ = 0;
        try {
            List<PcapNetworkInterface> nifList = Pcaps.findAllDevs();
            for (PcapNetworkInterface nif : nifList) {
                PcapHandle handle = nif.openLive(65536, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, 0);
                handle.loop(-1, listener);
                handle.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        size = summ;
    }

    @Override
    public void onStart() {
        if (ip != null) {
            getPacketByIP();
        } else {
            getAllPackets();
        }
    }

    @Override
    public void onStop() {

    }

    public static int getSize() {
        return size;
    }
}
