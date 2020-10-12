
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.*;

/**
 * @author ghippargi
 * <p>
 * This class is used to connect to AwsS3 instance and retrieve information.
 */
public class AwsS3Browser {


    public static void main(String args[]) {

      Console console = System.console();
      String accessKey = console.readLine("Enter your aws accessKey: ");
      String secretKey = console.readLine("Enter your aws secretKey: ");
      String region = console.readLine("Enter your aws region: ");
      String bucketName = console.readLine("Enter your aws bucketName: ");
      String instance = console.readLine("Enter your instance: ");
      String prefix = console.readLine("Enter your prefix for your aws files: ");
      
      AmazonS3 s3Client = getS3instance(accessKey, secretKey, region, bucketName, instance, prefix);
    }

    @SuppressWarnings("ALL")
    private static AmazonS3 getS3instance(String accessKey, String secretKey, 
                                           String region, String bucketName, String instance, String prefix) {

        System.out.println("Connecting to s3...");
        //use credentials to access AWS
        AmazonS3 s3Client = null;
        try {
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

            if (s3Client == null) {

                s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(region)).withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

                //check if the bucket is present or not
                Boolean isBucketPresent = s3Client.listBuckets().stream().anyMatch(bucket -> bucket.getName().equalsIgnoreCase(bucketName));

                //create bucket if not present
                if (!isBucketPresent) {
                    System.out.println("bucket:{} does not exist" + bucketName);
                    System.exit(0);
                }
                System.out.println("Connected to s3..." + bucketName);
                
                List<Map<String,Object>> imageList = getObjectList(s3Client,bucketName,instance, prefix);
                
               System.out.println("**********************************");
               System.out.println("Files retrieved from s3:" + imageList.size());
               System.out.println("**********************************");



            for (Map<String, Object> image : imageList) {

                System.out.println(image.get("mimeType")
                                + " " + image.get("name")
                                + " " + image.get("objectKey")
                );
            }

            }
        }
        catch (Exception ex) {
            System.out.println("Exception while connecting to S3 " + ex.getMessage());
            ex.printStackTrace();
            System.exit(0);

        }
        return s3Client;
    }
    
    private static List<Map<String,Object>> getObjectList(AmazonS3 s3, String bucketName, String instance, String prefix) throws Exception{

        List<Map<String,Object>> fileList = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;


        try {
                req = req.withPrefix(prefix);
                result = s3.listObjectsV2(req);
                System.out.println("object summaries:" + result.getObjectSummaries().size());

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {

                Map<String, Object> fileData = new HashMap<>();
                fileData.put("lastModified", objectSummary.getLastModified().getTime());


                try (S3Object object = s3.getObject(new GetObjectRequest(bucketName, objectSummary.getKey()))) {

                    if (object != null && object.getObjectContent() != null) {

                            String objectKey = object.getKey();
                            String objKeySubString = objectKey.substring(objectKey.indexOf(instance+"/")+instance.length()+1,objectKey.lastIndexOf("/"));
                            String[] idArr = objKeySubString.split("/");

                            fileData.put("businessId", idArr[0]);
                            if(idArr.length > 1) {

                                fileData.put("applicationId", idArr[1]);
                            }
                            fileData.put("objectKey", objectKey);
                            fileData.put("mimeType", object.getObjectMetadata().getContentType());
                            String fileName = object.getObjectMetadata().getUserMetadata().get("fileName");
                            fileName = formatImageName(fileName);
                            fileData.put("name", fileName);
                            fileList.add(fileData);

                    }
                }
                catch (IOException e) {
                    System.out.println("Error while fetching object for the object key objectSummary.getKey() from s3:{}");
                    throw new Exception("Failed to get object for object key - " + objectSummary.getKey() + " from AWS S3, error :", e);
                }
            }

        }
        catch (AmazonServiceException e) {
            System.out.println("Caught an AmazonServiceException getting objects , error:{}" + e);
            throw new Exception("Error while getting objects :", e);
        }

        catch (SdkClientException ex) {
            System.out.println("Caught an AmazonClientException while getting objects , error:{}" +  ex);
            throw new Exception("Error while getting an object :", ex);
        }

        return fileList;
    }
    
    private static String formatImageName(String imageName) {

        String fileExtn = FilenameUtils.getExtension(imageName);
        String fileName = FilenameUtils.getBaseName(imageName);

        fileName = fileName.replaceAll("([^\\w\\-])", "");
        if (fileName != null && fileName.length() > 40) {
            fileName = fileName.substring(0, 40);
        }
        return fileName + "." + fileExtn;
    }


}
