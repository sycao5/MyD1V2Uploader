package org.dataone.v2.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;
import org.dataone.client.v2.impl.MultipartMNode;
import org.dataone.client.v2.impl.MultipartCNode;


public class MyD1V2ClientUpdater {
	private static final Log log = LogFactory.getLog(MyD1V2ClientUpdater.class);

	//private static String mnBaseURL = "https://mn-demo-1.test.dataone.org/metacat/d1/mn";
	private static String mnBaseURL = "https://mn-demo-5.test.dataone.org/knb/d1/mn";
	private static String mnIdentifier = "urn:node:mnDemo5";

	private static String certificateLocation = "/tmp/x509up_u501";
	private static String nc_formatIdStr = "netCDF-3";
	private static String nc4_formatIdStr = "netCDF-4";

	private static final String filePath = "air.2m_monthly_2000_2010_mean.7.nc";

    public static void getCertificate(){
    	String clientCertificateLocation = certificateLocation;
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
    }

    public static void updateSystMeta(MNode v2MN, org.dataone.client.v1.MNode v1MN, String pidStr, String fmtstr){

		Identifier pid = new Identifier();
		pid.setValue(pidStr);

		try {
			SystemMetadata v1SysMeta = v1MN.getSystemMetadata(pid);
	        // Set format of uploading object
	        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
	        fmtid.setValue(fmtstr);
			v1SysMeta.setFormatId(fmtid);
			log.debug("v1SysMeta.formatId=" + v1SysMeta.getFormatId().toString());

			org.dataone.service.types.v2.SystemMetadata v2SysMeta = new org.dataone.service.types.v2.SystemMetadata();
			v2SysMeta = TypeMarshaller.convertTypeFromType(v1SysMeta, v2SysMeta.getClass());

			// Use v2 API (MNStorage.updateSystemMetadata()) -?
			v2MN.updateSystemMetadata(null, pid, v2SysMeta);
			log.debug("object system metadata is updated !");
		} catch (InstantiationException | IllegalAccessException | IOException
				| JiBXException | InvocationTargetException | InvalidToken | NotAuthorized | NotImplemented | ServiceFailure | NotFound | InvalidRequest | InvalidSystemMetadata e1) {
			e1.printStackTrace();
		}
    }

	/**
     * Create a SystemMetadata object for the given Identifier, using fake values
     * for the SystemMetadata fields.
     * @param pid            the identifier for the object to be used on the member node
     * @param formatId       the object format id
     * @param nref           reference to a member node
     * @param subjectStr     the subject to be used in sysmeta
     * @param file           the file object
     * @return the SystemMetadata object that is created
     */
    private static SystemMetadata generateSystemMetadataV1(String pid, String formatId, NodeReference nref, String subjectStr, File file) {
    	SystemMetadata sm = new SystemMetadata();
        Date created_time = new Date();

        sm.setSerialVersion(new BigInteger("1"));

        // Set identifier
		Identifier id = new Identifier();
		id.setValue(pid);
        sm.setIdentifier(id);

        // Set format of uploading object
        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue(formatId);
        sm.setFormatId(fmtid);

        // Set file size
        long size = file.length();
        sm.setSize(new BigInteger(String.valueOf(size)));

		// Set the checksum
		try {
			Checksum checksum = ChecksumUtil.checksum(new FileInputStream(file), "SHA-1");
			sm.setChecksum(checksum);
		} catch (NoSuchAlgorithmException e) {
			log.debug("Unknown algorithm. Error was: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException fnfe) {
			log.debug("Couldn't find file. Error was: " + fnfe.getMessage());
			fnfe.printStackTrace();
		}

        // Set subject and rightsHolder
		Subject mySubject = new Subject();
		mySubject.setValue(subjectStr);
        sm.setSubmitter(mySubject);
        sm.setRightsHolder(mySubject);

        // Set access policy
        AccessPolicy ap = AccessUtil.createSingleRuleAccessPolicy(new String[]{"public"}, new Permission[]{Permission.READ});
        sm.setAccessPolicy(ap);

        // Set replication policy
        ReplicationPolicy rp = new ReplicationPolicy();
		rp.setNumberReplicas(5);
		rp.setReplicationAllowed(true);
        sm.setReplicationPolicy(rp);

        // Set the upload and modification dates
		sm.setDateUploaded(created_time);
		sm.setDateSysMetadataModified(created_time);

		// Set the node fields
		sm.setOriginMemberNode(nref);
		sm.setAuthoritativeMemberNode(nref);

        return sm;
    }

	/**
     * Create a SystemMetadata object for the given Identifier, using fake values
     * for the SystemMetadata fields.
     * @param pid            the identifier for the object to be used on the member node
     * @param formatId       the object format id
     * @param nref           reference to a member node
     * @param subjectStr     the subject to be used in sysmeta
     * @param file           the file object
     * @return the SystemMetadata object that is created
     */
    private static org.dataone.service.types.v2.SystemMetadata generateSystemMetadata(String pid, String formatId, NodeReference nref, String subjectStr, File file) {
    	org.dataone.service.types.v2.SystemMetadata sm = new org.dataone.service.types.v2.SystemMetadata();
        Date created_time = new Date();

        sm.setSerialVersion(new BigInteger("1"));

        // Set identifier
		Identifier id = new Identifier();
		id.setValue(pid);
        sm.setIdentifier(id);

        // Set format of uploading object
        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue(formatId);
        sm.setFormatId(fmtid);

        // Set file size
        long size = file.length();
        sm.setSize(new BigInteger(String.valueOf(size)));

		// Set the checksum
		try {
			Checksum checksum = ChecksumUtil.checksum(new FileInputStream(file), "SHA-1");
			sm.setChecksum(checksum);
		} catch (NoSuchAlgorithmException e) {
			log.debug("Unknown algorithm. Error was: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException fnfe) {
			log.debug("Couldn't find file. Error was: " + fnfe.getMessage());
			fnfe.printStackTrace();
		}

        // Set subject and rightsHolder
		Subject mySubject = new Subject();
		mySubject.setValue(subjectStr);
        sm.setSubmitter(mySubject);
        sm.setRightsHolder(mySubject);

        // Set access policy
        AccessPolicy ap = AccessUtil.createSingleRuleAccessPolicy(new String[]{"public"}, new Permission[]{Permission.READ});
        sm.setAccessPolicy(ap);

        // Set replication policy
        ReplicationPolicy rp = new ReplicationPolicy();
		rp.setNumberReplicas(5);
		rp.setReplicationAllowed(true);
        sm.setReplicationPolicy(rp);

        // Set the upload and modification dates
		sm.setDateUploaded(created_time);
		sm.setDateSysMetadataModified(created_time);

		// Set the node fields
		sm.setOriginMemberNode(nref);
		sm.setAuthoritativeMemberNode(nref);

        return sm;
    }


	/**
	 * Upload an object to a Member Node
	 *
	 * @param memberNode  the Member Node object to communicate with
	 * @param sysmeta  the system metadata for the object
	 * @param file  the object itself
	 * @return success  true if the operation succeeds
	 */
	public static boolean createOnMN(MNode memberNode, org.dataone.service.types.v2.SystemMetadata sysmeta, File file) {

		boolean success = false;

		// Attempt to create the object on the Member Node
		try {
			Session session = new Session();
			InputStream fileInputStream = new FileInputStream(file);
			Identifier uploadedPid = memberNode.create(
					session, sysmeta.getIdentifier(), fileInputStream, sysmeta);
			success = true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IdentifierNotUnique e) {
			e.printStackTrace();
		} catch (InsufficientResources e) {
			e.printStackTrace();
		} catch (InvalidRequest e) {
			e.printStackTrace();
		} catch (InvalidSystemMetadata e) {
			e.printStackTrace();
		} catch (InvalidToken e) {
			e.printStackTrace();
		} catch (NotAuthorized e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		} catch (ServiceFailure e) {
			e.printStackTrace();
		} catch (UnsupportedType e) {
			e.printStackTrace();
		}

		return success;
	}


	/**
	 * Upload an object to a Member Node
	 *
	 * @param memberNode  the Member Node object to communicate with
	 * @param sysmeta  the system metadata for the object
	 * @param file  the object itself
	 * @return success  true if the operation succeeds
	 */
	public static boolean createOnMN(org.dataone.client.v1.MNode memberNode, org.dataone.service.types.v1.SystemMetadata sysmeta, File file) {

		boolean success = false;

		// Attempt to create the object on the Member Node
		try {
			InputStream fileInputStream = new FileInputStream(file);
			Identifier uploadedPid = memberNode.create(
					sysmeta.getIdentifier(), fileInputStream, sysmeta);
			success = true;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IdentifierNotUnique e) {
			e.printStackTrace();
		} catch (InsufficientResources e) {
			e.printStackTrace();
		} catch (InvalidRequest e) {
			e.printStackTrace();
		} catch (InvalidSystemMetadata e) {
			e.printStackTrace();
		} catch (InvalidToken e) {
			e.printStackTrace();
		} catch (NotAuthorized e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		} catch (ServiceFailure e) {
			e.printStackTrace();
		} catch (UnsupportedType e) {
			e.printStackTrace();
		}

		return success;
	}
	/**
	 * Main updater entry point
	 * @param args
	 * @throws IOException
	 * @throws JiBXException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, InvocationTargetException, JiBXException, IOException {
		getCertificate();

		// Get a MNode instance to the Member Node
		org.dataone.client.v2.MNode v2MN = null;
		org.dataone.client.v1.MNode v1MN = null;

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

		/**
		 * Update sysmeta from V1 to V2 using V2MN
		 */
/*		try {
			v1MN = D1Client.getMN(mnBaseURL);
			v2MN = org.dataone.client.v2.itk.D1Client.getMN(mnBaseURL);
		} catch (ServiceFailure e) {
			System.err.println("Couldn't create member node reference: " + e.getMessage());
			e.printStackTrace();
		}
		log.debug("Get MNnode correctly!");

		String pidStr = "mstmip_air.2m_monthly_2000_2010_mean.7.nc";
		updateSystMeta(v2MN, v1MN, pidStr, nc_formatIdStr);
*/

		/**
		 * Publish data to V2MN
		 */
		try {
			v1MN = org.dataone.client.v1.itk.D1Client.getMN(mnBaseURL);
			v2MN = org.dataone.client.v2.itk.D1Client.getMN(mnBaseURL);
		} catch (ServiceFailure e) {
			System.err.println("Couldn't create member node reference: " + e.getMessage());
			e.printStackTrace();
		}
		log.debug("Get V2MNnode correctly!");

		String pidStr ="1234";
		String formatIdStr = "text/plain";
		// Set the Node ID
		NodeReference nodeRef = new NodeReference();
		nodeRef.setValue(mnIdentifier);

	//	String fileName= "pubTest_ywExtractFacts.pl";
		String fileName= "air.2m_monthly_2000_2010_mean.7.nc";
		File file = new File(fileName);

		/** for v1 MNode*/
		SystemMetadata v1SysMeta = generateSystemMetadataV1(pidStr, formatIdStr, nodeRef, subjectStr, file);
		org.dataone.service.types.v2.SystemMetadata v2SysMeta = new org.dataone.service.types.v2.SystemMetadata();
		v2SysMeta = TypeMarshaller.convertTypeFromType(v1SysMeta, v2SysMeta.getClass());
		boolean success = MyD1V2ClientUpdater.createOnMN(v1MN, v1SysMeta, file);

		/** for v2 MNode*/
		//org.dataone.service.types.v2.SystemMetadata v2SysMeta = generateSystemMetadata(pidStr, formatIdStr, nodeRef, subjectStr, file);
		//boolean success = MyD1V2ClientUpdater.createOnMN(v2MN, v2SysMeta, file);

		if (success)
			System.out.println("Object is uploaded !");

	}
}
