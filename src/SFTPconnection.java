import com.jcraft.jsch.*;

public class SFTPconnection {

    Session m_session = null;
    Channel m_channel = null;

    public ChannelSftp getM_channelSftp() {
        return m_channelSftp;
    }

    private ChannelSftp m_channelSftp = null;

    public SFTPconnection(String hostname, String port, String username, String password, String work_directory ) throws JSchException, SftpException
    {
        JSch jsch = new JSch();
        m_session = jsch.getSession(username, hostname, Integer.parseInt(port));
        m_session.setPassword(password);

        m_session.setConfig("StrictHostKeyChecking", "no");
        m_session.connect();

        m_channel = m_session.openChannel("sftp");
        m_channel.connect();
        m_channelSftp = (ChannelSftp) m_channel;
        m_channelSftp.cd(work_directory);
    }

    public void close()
    {
        m_channelSftp.exit();
        m_session.disconnect();
    }
}
