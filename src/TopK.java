import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.io.File; 

class IdRating{
	double id;
	double rating;
	
	public IdRating(Double id, Double rating) {
		this.id = id;
		this.rating = rating;
	}
	
	public double getId() {
		return id;
	}

	public double getRating() {
		return rating;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return String.valueOf(this.id) + " " + String.valueOf(this.rating);
	}
}


class IdRatingSort implements Comparator<IdRating>{

	@Override
	public int compare(IdRating o1, IdRating o2) {
		return o1.rating<o2.rating?1:o1.rating>o2.rating?-1:0;
	}	
}

class Tuple implements Comparable<Tuple>{
	public Double id,criteria[],totalScore;

	public Tuple(Double id, Double[] criteria,Double[] V) {
		super();
		this.id = id;
		this.criteria = criteria;
		getThreshold(V);
	}
	
	public Tuple(Double id, Double[] criteria) {
		super();
		this.id = id;
		this.criteria = criteria;
		totalScore = 0d;		
	}
	
	public boolean equals(Object o){
		if(o instanceof Tuple){
			return this.id==((Tuple)o).id;
		}
		return false;
	}
	
	public int compareTo(Tuple o){
		return this.totalScore.compareTo(o.totalScore);
	}
	
	public Double getThreshold(Double V[]){
		totalScore = 0d;
		int n = V.length;
		for(int i=0;i<n;i++){
			totalScore+=criteria[i]*V[i];
		}
		return totalScore;
	}
	
	public Double getTotalScores() {
		return totalScore;
	}
	
	@Override
	public String toString() {
		String s = new String();
		s=s.concat("ID:"+id.intValue()+", and its total score: "+totalScore);	
		return s;
	}
}


public class TopK {
    /*
    * tableContent: list of key-(criteria_value) pairs for every criteria in CSV file except for the first criteria (i.e. item id)
    * Note: the first criteria is assumed to be primary key (i.e. item id)
    * */
	List<List<IdRating>> tableContent = new ArrayList<List<IdRating>>();
	
	
    /*
    * table : abstraction for CSV file
    * */
	HashMap<Double,Tuple> table = new HashMap<Double,Tuple>();
    /*
    * V : array of values associated with every criteria, used in scoring function
    * */
	Double V[];
	int criteria,totalRows,k,scannedRows;
	public TopK(int criteria, int k, Double V[]){
		this.criteria = criteria;
		this.k = k;
		this.V = V;
	}
	
	
	private Tuple createTuple(Double id){
		Tuple t = table.get(id);
        t.getThreshold(V);
		return t;
	}
	
	private Double getThreshold(Double criteria[]){
		Double threshold = 0d;
		int n = V.length;
		for(int i=0;i<n;i++){
			threshold+=criteria[i]*V[i];
		}
		return threshold;
	}
	
	private boolean indexCreation(String inputFile){		
		BufferedReader inputBufferedReader = null;
		FileReader fr = null;
		String inputLine = "";
        String splitRegex = ",";
		
		try {
			fr = new FileReader(inputFile);
			inputBufferedReader = new BufferedReader(fr);
			inputBufferedReader.readLine();                         // Ignore the names of columns
			for(int i = 0; i< criteria; i++){
				List<IdRating> tempList = new ArrayList<IdRating>();
				tableContent.add(tempList);
			}

			totalRows = 0;
			for(;(inputLine=inputBufferedReader.readLine())!=null;totalRows++) {
				String[] lineSplit = inputLine.split(splitRegex);
				Double attrValues[] = new Double[criteria];
                Double key = Double.valueOf(lineSplit[0]);
				for(int i=0;i<tableContent.size();i++){					
					attrValues[i] = Double.valueOf(lineSplit[i+1]);
					tableContent.get(i).add(new IdRating(key,Double.valueOf(lineSplit[i+1])));
				}				
				table.put(key,new Tuple(key, attrValues));
			}

			IdRatingSort sortObj = new IdRatingSort();
			//sorting each row of tableContent by ratings.
			//after this step, pre-processing is finished and the game starts.
			for(List<IdRating> tempList:tableContent){
				Collections.sort(tempList,sortObj);
			}
			
		} catch (FileNotFoundException e) {
			System.out.println("FileNotFoundException in reading input file");
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			System.out.println("IOException in reading line from input file");
			e.printStackTrace();
			return false;
		} finally {
			if(inputBufferedReader != null){
				try {
					inputBufferedReader.close();
				} catch (IOException e) {
					System.out.println("IOException in closing inputBufferedReader");
					e.printStackTrace();
                    return false;
				}
			}
			try{
				fr.close();
			}
			catch(Exception e){
                e.printStackTrace();
                return false;
			}
		}		
		return true;
	}
	
	public void init(String fileName){
		if(!indexCreation(fileName)){
            System.out.println("Couldn't create indexes on file (Bad CSV file)");
			System.exit(0);
		}
	}
	
	
	public Stack<Tuple> thresholdAlgo(){
		
		//Efficiency counter
		scannedRows=0;
		
		//Need to figure out what this is for
        HashSet<Double> keysSoFar = new HashSet<>();
        
        //Custom class which contains double: id and double: rating
		IdRating curRecord = null;
		
		//Need to figure out what this is for
		Double threshold = 0d;
		
		//store all the ratings in a given iteration.
		//double array with length of 10 (10 criteria)
		Double ratings[] = new Double[criteria];
		
	
		//Threshold Algorithm 
		
		//Initialise TopK
		PriorityQueue<Tuple> result = new PriorityQueue<>();
		
		//For each row in the table
		for (List<IdRating> list : tableContent) {
			scannedRows++;
			
			for (IdRating row : list) {
				curRecord = row;
				if (!result.contains(curRecord)) { 
					Tuple obj1 = createTuple(curRecord.getId());
					
					//Get total or actual?
					Double score1 = obj1.getTotalScores();
					
					
					if (result.size() >= this.k) {
						
						//Fix this
						Double score2 = (double) 3000; // lowest score in TopK obj result.peek();
						
						if (score2 < score1) {
							result.removeIf((Tuple tup) -> tup.getTotalScores() > score2);
							result.add(obj1);
						}
						
						//check
						threshold = obj1.getThreshold(V);
						if (threshold < score2) {
							break;
						}
						
					} else {
						result.add(obj1);
					}
				}
			}
		}
		
		
			
		Stack<Tuple> s = new Stack<Tuple>();
		while(!result.isEmpty()){			
			s.push(result.poll());
		}
		
		return s;
	}
	
	public void printStack(Stack<Tuple> s){
		while(!s.isEmpty()){
			
            System.out.println(s.pop());
		}
	}
		
	public static void main(String[] args) throws IOException {
		
		long totalTime=0;
        
      	File myObj = new File("fileList.txt");
      	
      	Scanner myReader = new Scanner(myObj);
      	
      	int count=0;
      	while (myReader.hasNextLine()) {
	        count++;
	        String fileName = myReader.nextLine();
	        
	        // the top k items. 10 by default.
	        int k=10;
	        // the number of criterias to be considered. 10 by default.
	        int n=10;     
	        //weights for criterias. Set as 1.0 for all considered criterias by default.         
	        Double V[] = new Double[n];

	        for(int i=0;i<=n-1;i++){
	            V[i]=1.0;
	        }

	        TopK obj = new TopK(n,k,V);

	        //Initiate TA algorithm
	        obj.init(fileName);

	        long startTime = System.currentTimeMillis();
	        System.out.println(fileName+": Top "+k+" are:");
	        obj.printStack(obj.thresholdAlgo());
	        long endTime = System.currentTimeMillis(); 

	        System.out.println("There are "+obj.totalRows+" rows in total, and only "+obj.scannedRows+" rows are scanned");   
	    	
	    	totalTime+=endTime - startTime;
	    	System.out.println("Running time：" + (endTime - startTime) + "ms \n");
	    }
	    System.out.println("Average running time:" + totalTime/count + "ms");
    }
}
