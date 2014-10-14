package edu.indiana.dlib.fedora;

import com.yourmediashelf.fedora.client.FedoraClient;
import com.yourmediashelf.fedora.client.FedoraClientException;
import com.yourmediashelf.fedora.client.FedoraCredentials;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FedoraModsFetcher {

    public static void main(String [] args) throws Exception {
        Properties p = new Properties();
        p.load(new FileInputStream("collection-limits.properties"));

        File outputDir = new File(p.getProperty("output-dir"));
        outputDir.mkdirs();
        System.out.println("Output directory: " + outputDir.getAbsolutePath());

        FedoraModsFetcher f = new FedoraModsFetcher(new FedoraClient(new FedoraCredentials(p.getProperty("fedora-url"), "", "")));
        System.out.println("Fedora URL:       " + p.getProperty("fedora-url"));

        boolean blacklist = p.getProperty("inclusion").equals("blacklist");

        // walk through all the collections
        List<String> collectionPids = f.getCollections();
        Collections.sort(collectionPids);
        System.out.println(collectionPids.size() + " collection found.");
        for (String collectionPid : collectionPids) {
            int limit = 0;
            if (p.containsKey(collectionPid)) {
                String limitStr = p.getProperty(collectionPid);
                try {
                    limit = Integer.parseInt(limitStr);
                } catch (Throwable t) {
                    // we'll interpret the non-number as "all"
                }
            } else if (blacklist) {
                limit = -1;
            }
            if (limit == 0) {
                //System.out.println("Skipping collection " + collectionPid + ".");
            } else {
                List<String> pids = f.getCollectionMembers(collectionPid);
                System.out.println("Fetching " + (-1 == limit ? "all" : limit) + " of " + pids.size() + " MODS records from collection " + collectionPid + "...");
                File cDir = new File(outputDir, collectionPid.replace(":", "_"));
                cDir.mkdir();

                for (int i = 0; i != limit && i < pids.size(); i++) {
                    final String pid = pids.get(i);
                    File outputFile = new File(cDir, pid.replace(":", "_"));
                    if (!outputFile.exists()) {
                        try {
                            IOUtils.copy(f.getModsRecordForPid(pid), new FileOutputStream(outputFile));
                            System.out.println("  " + pid + " exported");
                        } catch (Throwable t) {
                            System.out.println("Error exporting MODS for " + pid + "!");
                            t.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private FedoraClient fc;

    public FedoraModsFetcher(FedoraClient fedoraClient) {
        this.fc = fedoraClient;
    }

    /**
     * Finds all collections in the Fedora repository by querying the resource index
     * for all objects that "hasModel" "info:fedora/cmodel:collection".
     * @return a List of pids representing collection objects
     */
    public List<String> getCollections() throws Exception {
        return getSubjects(fc, "cmodel:collection", "info:fedora/fedora-system:def/model#hasModel");
    }

    /**
     * Finds all objects in the Fedora repository that belong to a given collection
     * by querying the resource index.
     * @return a List of pids representing objects in a collection
     */
    public List<String> getCollectionMembers(String collectionPid) throws Exception {
        return getSubjects(fc, collectionPid, "info:fedora/fedora-system:def/relations-external#isMemberOfCollection");
    }

    /**
     * Gets the mods dissemination.  This is like requesting the following URL:
     * http://[fedora-host]:[fedora-port]/fedora/objects/[pid]/methods/bdef:iudlDescMetadata/getMODS?version=
     * @param pid
     * @return
     * @throws FedoraClientException
     */
    public InputStream getModsRecordForPid(String pid) throws FedoraClientException {
        return FedoraClient.getDissemination(pid, "bdef:iudlDescMetadata", "getMODS").methodParam("version", "").execute(fc).getEntityInputStream();
    }

    /**
     * Gets the subjects of the given predicate for which the object is give given object.
     * For example, a relationship like "[subject] follows [object]" this method would always
     * return the subject that comes before the given object.
     * @param fc the fedora client that mediates access to fedora
     * @param objectPid the pid of the object that will have the given predicate relationship
     * to all subjects returned.
     * @param predicate the predicate to query
     * @return the URIs of the subjects that are related to the given object by the given
     * predicate
     */

    public static List<String> getSubjects(FedoraClient fc, String objectPid, String predicate) throws Exception {
        if (predicate == null) {
            throw new NullPointerException("predicate must not be null!");
        }
        String itqlQuery = "select $subject from <#ri> where $subject <" + predicate + "> " + (objectPid != null ? "<info:fedora/" + objectPid + ">" : "$other");
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qsubject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        return pids;
    }
}
