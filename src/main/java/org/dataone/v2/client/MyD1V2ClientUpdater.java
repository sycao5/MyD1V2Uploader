package org.dataone.v2.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.v1.MNode;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

public class MyD1V2ClientUpdater {
	private static final Log log = LogFactory.getLog(MyD1V2ClientUpdater.class);

	private static String mnBaseURL = "https://mn-demo-1.test.dataone.org/metacat/d1/mn";
	private static String certificateLocation = "/tmp/x509up_u501";
	private static String nc_formatIdStr = "netCDF-3";
	private static String nc4_formatIdStr = "netCDF-4";

    public static void getCertificate(){
    	String clientCertificateLocation = certificateLocation;
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
    }

    public static void updateSystMeta(org.dataone.client.v2.MNode v2MN, MNode v1MN, String pidStr, String fmtstr){

		Identifier pid = new Identifier();
		pid.setValue(pidStr);

		try {
			SystemMetadata v1SysMeta = v1MN.getSystemMetadata(pid);
	        // Set format of uploading object
	        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
	        fmtid.setValue(fmtstr);
			v1SysMeta.setFormatId(fmtid);
			System.out.println("v1SysMeta.formatId=" + v1SysMeta.getFormatId());

			org.dataone.service.types.v2.SystemMetadata v2SysMeta = new org.dataone.service.types.v2.SystemMetadata();
			v2SysMeta = TypeMarshaller.convertTypeFromType(v1SysMeta, v2SysMeta.getClass());

			// Use v2 API to update system metadata
			v2MN.updateSystemMetadata(null, pid, v2SysMeta);

	        // Set format of uploading object
	  //      ObjectFo1rmatIdentifier fmtid = new ObjectFormatIdentifier();
	  //      fmtid.setValue(fmtstr);
	//		v2SysMeta.setFormatId(fmtid);
	//		System.out.println("v2SysMeta.formatId=" + v2SysMeta.getFormatId());

			log.debug("object system metadata is updated !");
		} catch (InstantiationException | IllegalAccessException | IOException
				| JiBXException | InvocationTargetException | InvalidToken | NotAuthorized | NotImplemented | ServiceFailure | NotFound | InvalidRequest | InvalidSystemMetadata e1) {
			e1.printStackTrace();
		}
    }

	/**
	 * Main updater entry point
	 * @param args
	 */
	public static void main(String[] args) {
		getCertificate();

		// Get a MNode instance to the Member Node
		org.dataone.client.v2.MNode v2MN = null;
		MNode v1MN = null;

		// Pull the subject DN out of the certificate for use in system metadata
		String subjectStr = "No subject yet";
		CertificateManager certManager = CertificateManager.getInstance();
		try {
			subjectStr = certManager.getSubjectDN(certManager.loadCertificateFromFile(certificateLocation));
			log.debug(subjectStr);
		} catch (IOException ioe) {
			log.debug("Couldn't find file. Error was: " + ioe.getMessage());
			ioe.printStackTrace();
		}

		try {
			v1MN = D1Client.getMN(mnBaseURL);
			v2MN = org.dataone.client.v2.itk.D1Client.getMN(mnBaseURL);
		} catch (ServiceFailure e) {
			System.err.println("Couldn't create member node reference: " + e.getMessage());
			e.printStackTrace();
		}
		System.out.println("Get MNnode correctly!");

		String pidStr = "mstmip_air.2m_monthly_2000_2010_mean.7.nc";
		updateSystMeta(v2MN, v1MN, pidStr, nc_formatIdStr);

		//	Update mstmip examples system metadata
/*		Identifier pid = new Identifier();
		String pidStr = null;
		for(int i = 1; i <=12 ; i ++){
			pidStr = "mstmip_cruncep_monthly_2000_2010_mean_Tair" +i +".nc";
			pid.setValue(pidStr);
			updateSystMeta(memberNode, pid, nc_formatIdStr);
		}

		for(int i = 1; i <=12 ; i ++){
			pidStr = "mstmip_cruncep_monthly_2000_2010_mean_rain_" +i +".nc";
			pid.setValue(pidStr);
			updateSystMeta(memberNode, pid, nc_formatIdStr);
		}

		for(int i = 1; i <=12 ; i ++){
			pidStr = "mstmip_air.2m_monthly_2000_2010_mean." +i +".nc";
			pid.setValue(pidStr);
			updateSystMeta(memberNode, pid, nc_formatIdStr);
		}

		for(int i = 1; i <=12 ; i ++){
			pidStr = "mstmip_apcp_monthly_2000_2010_mean." +i +".nc";
			pid.setValue(pidStr);
			updateSystMeta(memberNode, pid, nc_formatIdStr);
		}
	*/
	}
}
