import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.IOUtils;
import streamserve.connector.StrsConfigVals;
import streamserve.connector.StrsInConnectable;
import streamserve.connector.StrsInDataQueueable;
import streamserve.connector.StrsServiceable;

import java.io.*;
import java.rmi.RemoteException;
import java.util.Vector;

public class SFTPinConnector implements StrsInConnectable {

    static final String PROPERTY_SERVER = "Server";
    static final String PROPERTY_SERVER_PORT = "Server port";
    static final String PROPERTY_USER = "User";
    static final String PROPERTY_USER_PASSWORD = "User password";
    static final String PROPERTY_FOLDER = "Folder";
    static final String PROPERTY_PATTERN = "File name pattern";             // *.txt
    static final String PROPERTY_SAVEPATH = "Save Path";  // file tree

    private StrsServiceable m_service;
    private String m_servername = "";
    private String m_serverport = "";
    private String m_username = "";
    private String m_userpassword = "";
    private String m_folder = "";
    private String m_pattern = "";
    private String m_savepath = "";

    private SFTPconnection m_sftPconnection;


    @Override
    public boolean strsiStart(StrsConfigVals strsConfigVals) throws RemoteException {

        if (strsConfigVals == null)
        {
            logError("strsConfigVals == null");
            return false;
        }

        LoadStrsConfigValues(strsConfigVals);

        logDebug("Server name : "+  m_servername);
        logDebug("Server port : "+  m_serverport);
        logDebug("User : "+  m_username);
        logDebug("User password : "+  m_userpassword);
        logDebug("Folder  : "+  m_folder);
        logDebug("File pattern  : "+  m_pattern);
        logDebug("Save path: " + m_savepath);

        try
        {
            m_sftPconnection = new SFTPconnection(m_servername, m_serverport, m_username, m_userpassword, m_folder);
        }

        catch (JSchException e)
        {
            logError(e.getMessage());
            return false;
        }
        catch (SftpException e)
        {
            logError(e.getMessage());
            return false;
        }

        logInfo("Connection to " + m_servername + " is established");
        return true;
    }

    @Override
    public boolean strsiPoll(StrsInDataQueueable inDataQueue) throws RemoteException {

        Vector<ChannelSftp.LsEntry> list = null;

        if (m_pattern.isEmpty() )
        {
            logInfo("The pattern is empty. No files will be downloaded");
            return false;
        }
        try {
            list = m_sftPconnection.getM_channelSftp().ls(m_pattern);

            for(ChannelSftp.LsEntry entry : list)
            {
                //m_sftPconnection.m_channelSftp.get(entry.getFilename(), TMP_FOLDER + entry.getFilename());
                InputStream in = m_sftPconnection.getM_channelSftp().get(entry.getFilename());
                m_sftPconnection.getM_channelSftp().rm(entry.getFilename());

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                org.apache.commons.io.IOUtils.copy(in, baos);
                byte[] bytes = baos.toByteArray();          // we can read multiple times from this byte array

                if (!m_savepath.isEmpty() && m_savepath.length() > 0)
                {
                    FileOutputStream os = new FileOutputStream(m_savepath + "\\" + entry.getFilename());
                    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                    IOUtils.copy(bais, os);
                    os.close();

                    logDebug(entry.getFilename() + " file saved in: " + m_savepath + "\\" + entry.getFilename());
                }

                //inDataQueue.signalEvent(StrsInDataQueueable.INEVENT_JOBBEGIN);
                //inDataQueue.putArray(IOUtils.toByteArray(in));

                ByteArrayInputStream bais_cc = new ByteArrayInputStream(bytes);
                if (!inDataQueue.putString(IOUtils.toString(bais_cc)) )
                    logError("putString failed");
                in.close();
                inDataQueue.signalEvent(StrsInDataQueueable.INEVENT_EOF);
                //inDataQueue.signalEvent(StrsInDataQueueable.INEVENT_JOBEND);
                logDebug(entry.getFilename() + " file was sent to Communication Server ");

            }

        } catch (SftpException e) {
            logError(e.getMessage());
            return false;
        } catch (IOException e) {
            logError(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public boolean strsiStop() throws RemoteException {

        m_sftPconnection.close();
        return true;
    }


    private void logError(String message) throws RemoteException
    {
        if (m_service != null)
        {
            m_service.writeMsg(StrsServiceable.MSG_ERROR, 0, "SFTPInConnector ERROR: " + message);
        }
        else
        {
            System.out.println(message);
        }
    }

    private void logInfo(String message) throws RemoteException
    {
        if (m_service != null)
        {
            m_service.writeMsg(StrsServiceable.MSG_INFO, 0, "SFTPInConnector INFO: " + message);
        }
        else
        {
            System.out.println(message);
        }
    }

    private void logDebug(String message) throws RemoteException
    {
        if (m_service != null)
        {
            m_service.writeMsg(StrsServiceable.MSG_DEBUG, 0, "SFTPInConnector DEBUG: " + message);
        }
        else
        {
            System.out.println(message);
        }
    }

    private void LoadStrsConfigValues(StrsConfigVals strsConfigVals)
    {
        m_service = strsConfigVals.getStrsService();

        String servername = strsConfigVals.getValue(PROPERTY_SERVER);
        if (!servername.isEmpty())
        {
            m_servername = servername;
        }

        String serverport = strsConfigVals.getValue(PROPERTY_SERVER_PORT);
        if (!serverport.isEmpty())
        {
            m_serverport = serverport;
        }

        String username = strsConfigVals.getValue(PROPERTY_USER);
        if (!username.isEmpty())
        {
            m_username = username;
        }

        String userpassword = strsConfigVals.getValue(PROPERTY_USER_PASSWORD);
        if (!userpassword.isEmpty())
        {
            m_userpassword = userpassword;
        }

        String working_folder = strsConfigVals.getValue(PROPERTY_FOLDER);
        if (!working_folder.isEmpty())
        {
            m_folder = working_folder;
        }

        String pattern = strsConfigVals.getValue(PROPERTY_PATTERN);
        if (!pattern.isEmpty()) {
            m_pattern = pattern;
        }

        String savepath = strsConfigVals.getValue(PROPERTY_SAVEPATH);
        if (!pattern.isEmpty())  {
            m_savepath = savepath;
        }
    }
}
