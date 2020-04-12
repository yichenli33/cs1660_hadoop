import java.awt.EventQueue;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingConstants;
import java.awt.Font;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.TimerTask;
import java.awt.event.ActionEvent;
import javax.swing.JLayeredPane;
import javax.swing.JScrollPane;
import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JTextField;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

public class SearchGUI {
	
	public String bucketName = "dataproc-staging-us-west1-818742901208-uyi6ac2g";
	public String projectId = "coral-silicon-273701";
	public String apiKey = "AIzaSyBjKqwcL24FxKpQrPcpsFd2Qhcue8B5nWc";
	public String jobId;
	public String clusterRegion = "us-west1";
	public String clusterName = "hadoop-cluster-1";
	public static String accessToken;
	private JFrame frame;
	private JTextField searchTextField;
	private JTable tableSearch = null;
	private JLabel lblLoading;
	private JButton btnGoBackToSearch;
	private JLabel executionTimeLabel;
	private JButton btnSearchForTerm;
	private File files[];
	private String InvertedIndexResult;
	public String startTime;
	public String endTime;

	
	private SearchGUI getThis() {
		return this;
	}
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		accessToken = System.getenv("ACCESS_TOKEN");
		
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					SearchGUI window = new SearchGUI();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public static String HttpRequestMethod(String type, String url, String contentType, HttpEntity entity, String accessToken) {
		try {
			HttpClient client = HttpClientBuilder.create().build();
			HttpRequestBase request;
			if(type.equals("POST")) {
				request = new HttpPost(url);
			}
			else if(type.equals("DELETE")) {
				request = new HttpDelete(url);
			}
			else {
				request = new HttpGet(url);
			}
			
			if(contentType != null) {
				request.addHeader("Content-Type", contentType);
			}
			
			if (entity != null) {
				if (type.equals("POST")) {
					((HttpPost)request).setEntity(entity);	
				}
			
			}
			
			if(type.equals("GET")) {
				request.addHeader("Cache-Control", "no-cache, max-age=0");
				
			}
			
			HttpResponse response = client.execute(request);
			if (type.equals("DELETE")) {
				return null;
			}
			InputStream input = response.getEntity().getContent();
			String body = IOUtils.toString(input, "UTF-8");
			return body;
			
			
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	
	public void InvertedIndexJob(boolean flag) {
		if(flag ==true) {
			lblLoading.setText("<html>Job Success.<br/> Constructed Inverted Index!<br/></html>");
		}
		else {
			lblLoading.setText("Job Failed");
		}
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			Date startDate = formatter.parse(startTime.substring(0, 19));
			Date endDate = formatter.parse(endTime.substring(0, 19));
		    long diff = (endDate.getTime() - startDate.getTime()) / 1000;
		    executionTimeLabel.setText("Execution Time: " + Long.toString(diff));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if(!flag) return;
		
		InvertedIndexResult = HttpRequestMethod("GET", "https://storage.googleapis.com/storage/v1/b/" + bucketName + "/o/" + "II.txt" +"?alt=media", null, null, accessToken);
		btnSearchForTerm.setVisible(true);
	}
	/**
	 * Create the application.
	 */
	public SearchGUI() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frame = new JFrame();
		frame.setBounds(100, 100, 950, 620);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		JLayeredPane layeredPane = new JLayeredPane();
		layeredPane.setBounds(0, 0, 932, 533);
		frame.getContentPane().add(layeredPane);
		
		JLayeredPane layeredPane_1 = new JLayeredPane();
		layeredPane_1.setBounds(0, 37, 933, 538);
		layeredPane_1.setVisible(false);
		frame.getContentPane().add(layeredPane_1);
		
		JLayeredPane layeredPane_2 = new JLayeredPane();
		layeredPane_2.setBounds(0, 37, 940, 538);
		layeredPane_2.setVisible(false);
		frame.getContentPane().add(layeredPane_2);
		
		JFileChooser fileChooser = new JFileChooser();
		fileChooser.setMultiSelectionEnabled(true);
		
		JLabel lblFileToBe = new JLabel("File to Upload");
		lblFileToBe.setBounds(0, 167, 900, 230);
		layeredPane.add(lblFileToBe);
		lblFileToBe.setVerticalAlignment(SwingConstants.TOP);
		lblFileToBe.setFont(new Font("Tahoma", Font.PLAIN, 18));
		lblFileToBe.setHorizontalAlignment(SwingConstants.CENTER);
		
		JButton btnChooseFile = new JButton("Choose File");
		btnChooseFile.setBounds(367, 94, 440, 80);
		layeredPane.add(btnChooseFile);
		
		JButton btnLoadEngine = new JButton("Load Engine");
		btnLoadEngine.setVisible(false);
		btnLoadEngine.setBounds(370, 165, 117, 29);
		layeredPane.add(btnLoadEngine);
		btnLoadEngine.setFont(new Font("Tahoma", Font.PLAIN, 22));
		
		lblLoading = new JLabel("Loading...");
		lblLoading.setBounds(10, 122, 117, 21);
		lblLoading.setVisible(false);
		layeredPane.add(lblLoading);
		lblLoading.setFont(new Font("Tahoma", Font.PLAIN, 20));
		lblLoading.setHorizontalAlignment(SwingConstants.CENTER);
		
		
		btnSearchForTerm = new JButton("Search For Term");
		btnSearchForTerm.setVisible(false);
		btnSearchForTerm.setFont(new Font("Tahoma", Font.PLAIN, 14));
		btnSearchForTerm.setBounds(172, 205, 515, 29);
		layeredPane.add(btnSearchForTerm);
		
		JLabel lblEnterSearchTerm = new JLabel("Enter Search Term:");
		lblEnterSearchTerm.setBounds(10, 120, 265, 85);
		layeredPane_1.add(lblEnterSearchTerm);
		lblEnterSearchTerm.setHorizontalAlignment(SwingConstants.CENTER);
		lblEnterSearchTerm.setFont(new Font("Tahoma", Font.BOLD, 18));
		searchTextField = new JTextField();
		searchTextField.setBounds(320, 150, 200, 22);
		layeredPane_1.add(searchTextField);
		searchTextField.setFont(new Font("Tahoma", Font.PLAIN, 14));
		searchTextField.setColumns(10);
		
		
		JButton btnSearch = new JButton("Search!");	
		btnSearch.setBounds(550, 145, 80, 40);
		layeredPane_1.add(btnSearch);
		btnSearch.setFont(new Font("Tahoma", Font.PLAIN, 22));
		
		btnGoBackToSearch = new JButton("Go Back To Search");
		btnGoBackToSearch.setBounds(685, 0, 187, 22);
		layeredPane_2.add(btnGoBackToSearch);
		btnGoBackToSearch.setFont(new Font("Tahoma", Font.PLAIN, 18));
		
		JLabel lblSearchedTerm = new JLabel("Searched Term");
		lblSearchedTerm.setBounds(58, 0, 680, 48);
		layeredPane_2.add(lblSearchedTerm);
		lblSearchedTerm.setFont(new Font("Tahoma", Font.PLAIN, 12));
		
		JLabel executionTimeLabel = new JLabel("Search Elapsed Time");
		executionTimeLabel.setBounds(58, 51, 200, 48);
		layeredPane_2.add(executionTimeLabel);
		executionTimeLabel.setFont(new Font("Tahoma", Font.PLAIN, 12));
		
		
		JLabel lblTermNotExist = new JLabel("Term Does Not Exist!");
		lblTermNotExist.setVisible(false);
		lblTermNotExist.setBounds(320, 162, 342, 120);
		layeredPane_2.add(lblTermNotExist);
		lblTermNotExist.setFont(new Font("Tahoma", Font.BOLD, 22));
		lblTermNotExist.setHorizontalAlignment(SwingConstants.CENTER);
		
		
		tableSearch = new JTable();
		tableSearch.setBounds(200, 250, 600, 300);
		tableSearch.setRowHeight(20);
		tableSearch.setFont(new Font("Tahoma", Font.PLAIN, 12));	
		layeredPane_2.add(tableSearch);
		
		btnChooseFile.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				int res = fileChooser.showSaveDialog(null);
				if (res == JFileChooser.APPROVE_OPTION) {
					String text = "";
					files = fileChooser.getSelectedFiles();
					text = "<html><div style='text-align: center;'>";
					for (int n = 0; n < files.length; n++)
						text += files[n] + "<br/>";
					text += "</div></html>";
					
					lblFileToBe.setText(text);
					btnLoadEngine.setVisible(true);
				}
			}
		});
		
		btnLoadEngine.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				lblFileToBe.setVisible(false);
				btnChooseFile.setVisible(false);
				lblLoading.setVisible(true);
				btnLoadEngine.setVisible(false);
				
				for (File f: files) {
					String obj = HttpRequestMethod("POST", "https://storage.googleapis.com/upload/storage/v1/b/" + bucketName + "/o??uploadType=media&name=input/" + f.getName(), "application/octet-stream", new FileEntity(f), accessToken);
					if(obj == null) {
						lblLoading.setText("Failed to upload files to bucket");
						return;
					}
				}
				lblLoading.setText("<html>Files Uploaded to bucket.<br/>Now Populationg InvertedIndex.txt on Cluster</html>");
				String jsonBody = "{\"projectId\": \"" + projectId + "\"," +"\"job\": {\"placement\": {\"clusterName\": \"" + clusterName + "\"},\"hadoopJob\": {\"jarFileUris\": [\"gs://" + bucketName +"/JAR/InvertedIndex.jar\"],\"args\": [\"gs://" + bucketName + "/input\",\"gs://" + bucketName + "/IIOutput\"],\"mainClass\": \"InvertedIndex\"}}}";
				try {
					JSONObject obj = new JSONObject(HttpRequestMethod("POST", "https://dataproc.googleapis.com/v1/projects/" + projectId +"/regions/" + clusterRegion +"/jobs:submit" + "?key=" + apiKey, "application/json", new StringEntity(jsonBody), accessToken));
					jobId = obj.getJSONObject("reference").getString("jobId");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				java.util.Timer timer = new java.util.Timer();
				RecurrentTask task = new RecurrentTask(getThis(), "InvertedIndex");
				timer.scheduleAtFixedRate(task, 0, 5000);
			}
		});
		
		btnSearch.addActionListener(new ActionListener() {	
			public void actionPerformed(ActionEvent e) {
				btnSearch.setText("Searching...");
				
				long start = System.currentTimeMillis();
			      
				String wordSearched = searchTextField.getText();
				List<String> docList = new ArrayList<String>(files.length);
				List<String> frequencyList = new ArrayList<String>(files.length);
				
				Scanner scanner = new Scanner(InvertedIndexResult);
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					String[] array = line.split("\t");
					if(wordSearched.equals(array[0])) {
						System.out.println("Found term");
						for(int n=0;n < files.length; n++) {
							if(!array[0].equals(wordSearched)) 
								break;
							
							docList.add(array[1]);
							frequencyList.add(array[2]);
							
							if(!scanner.hasNextLine()) 
								break;
							
							line = scanner.nextLine();
							array = line.split("\t");
						}
						break;
					}
				}
				scanner.close();
				long end = System.currentTimeMillis();
				
				lblSearchedTerm.setText("Searched Term: " + wordSearched);
				executionTimeLabel.setText("Search Elapsed Time: " + Float.toString((end - start) / 1000F));
				
				if(docList.size() == 0) {
					tableSearch.setModel(new DefaultTableModel());
					lblTermNotExist.setVisible(true);
				}
				else {						
					DefaultTableModel model = new DefaultTableModel();
				    model.setColumnIdentifiers(new Object[] {"Doc Name", "Frequency"});
				    model.addRow(new Object[] {"Doc Name", "Frequency"});
				    for(int n=0;n<docList.size();n++) 
				    	model.addRow(new Object[] {docList.get(n), frequencyList.get(n)});
					tableSearch.setModel(model);
				}
				layeredPane_1.setVisible(false);
				layeredPane_2.setVisible(true);	
			}
		});
		
		btnGoBackToSearch.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				btnSearch.setText("Search!");
				lblTermNotExist.setVisible(false);
				layeredPane_2.setVisible(false);
				layeredPane_1.setVisible(true);
			}
		});
		
		btnSearchForTerm.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				layeredPane.setVisible(false);
				layeredPane_1.setVisible(true);
			}
		});
	}
}

class RecurrentTask extends TimerTask{
	private SearchGUI guiClass;
	private String job;
	
	public RecurrentTask(SearchGUI curr_instance, String job) {
		this.guiClass = curr_instance;
		this.job = job;
	}
	
	public void run() {
		JSONObject obj = new JSONObject(SearchGUI.HttpRequestMethod("GET","https://dataproc.googleapis.com/v1beta2/projects/" + guiClass.projectId +"/regions/" + guiClass.clusterRegion +"/jobs/" + guiClass.jobId + "?key=" + guiClass.apiKey, null, null, guiClass.accessToken) );
		
		if(obj.has("done")) {
//			guiClass.logPosition = obj.getString("driverOutputResourceUri");
			System.out.println(obj.getJSONObject("status").getString("state"));
			boolean success = obj.getJSONObject("status").getString("state").equals("DONE") ? true : false;
			guiClass.endTime = obj.getJSONObject("status").getString("stateStartTime");
			JSONArray arr = obj.getJSONArray("statusHistory");
			for(int i=0;i<arr.length();i++) {
				if(arr.getJSONObject(i).getString("state").equals("PENDING")) {
					guiClass.startTime = arr.getJSONObject(i).getString("stateStartTime");
					break;
				}
			}
			if(job.equals("InvertedIndex"))
				guiClass.InvertedIndexJob(success);
			this.cancel();
		}
	}
	
}
