package pvehiculos3;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import org.bson.Document;

public class Pvehiculos3 {

    //VARIABLES OBJECTDB
    private static Vendas objV;
    private static double id;
    private static String dni;
    private static String codvh;
    private static double tasas;
    //VARIABLES ORACLE VEHICULOS
    private static String nomv;
    private static java.math.BigDecimal prezoorixe;
    private static java.math.BigDecimal anomatricula;
    //VARIABLES ORACLE CLIENTES
    private static String nomec;
    private static java.math.BigDecimal ncompras;
    //PF
    private static double pf;
    
    public static void Ejercicio() throws SQLException {

        //CONEXIÓN OBJECTDB
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("$objectdb/db/vehicli.odb");
        
        EntityManager em = emf.createEntityManager();
        
        em.getTransaction().begin();
        //COMO VAMOS A OBTENER TODOS LOS RESULTADOS DE OBJECTDB, NECESITAMOS HACER UNA LISTA PARA RECORRERLA
        TypedQuery<Vendas> query
                = em.createQuery("SELECT p FROM Vendas p", Vendas.class);
        
        List<Vendas> results = query.getResultList();
        
        for (Vendas z : results) {
            
            id = z.getId();
            System.out.println("ID:" + id);
            dni = z.getDni();
            System.out.println("DNI: " + dni);
            codvh = z.getCodvh();
            System.out.println("CODVH: " + codvh);
            tasas = z.getTasas();
            System.out.println("TASAS: " + tasas);

            //CONTINUAMOS CON ORACLE:
            Connection conn;
            String driver = "jdbc:oracle:thin:";
            String host = "localhost.localdomain"; // tambien puede ser una ip como "192.168.1.14"
            String porto = "1521";
            String sid = "orcl";
            String usuario = "hr";
            String password = "hr";
            String url = driver + usuario + "/" + password + "@" + host + ":" + porto + ":" + sid;
            
            conn = DriverManager.getConnection(url);
            
            PreparedStatement psV = conn.prepareStatement("select * from vehiculos where idv = ?");
            
            psV.setString(1, codvh);
            
            psV.executeUpdate();
            
            ResultSet rsV = psV.getResultSet();
            
            while (rsV.next()) {
                
                nomv = rsV.getString("nomv");
                System.out.println("NOMV: " + nomv);

                //AHORA OBTENEMOS LOS DATOS DE LA VARIABLE OBJETO
                java.sql.Struct objSQL = (java.sql.Struct) rsV.getObject(3);
                
                Object[] campos = objSQL.getAttributes();
                
                prezoorixe = (java.math.BigDecimal) campos[0];
                System.out.println("PREZOORIXE: " + prezoorixe);
                anomatricula = (java.math.BigDecimal) campos[1];
                System.out.println("ANOMATRICULA: " + anomatricula);
                
                System.out.println("******************************************");

                //AHORA LOS DATOS DE CLIENTES EN ORACLE
                PreparedStatement psC = conn.prepareStatement("select * from clientes where idcli = ?");
                
                psC.setString(1, dni);
                
                psC.executeUpdate();
                
                ResultSet rsC = psC.getResultSet();
                
                while (rsC.next()) {

                    //AHORA OBTENEMOS LOS DATOS DE LA VARIABLE OBJETO
                    java.sql.Struct objSQL2 = (java.sql.Struct) rsV.getObject(2);
                    
                    Object[] campos2 = objSQL2.getAttributes();
                    
                    nomec = (String) campos[0];
                    System.out.println("NOMEC: " + nomec);
                    ncompras = (java.math.BigDecimal) campos[1];
                    System.out.println("NCOMPRAS: " + ncompras);
                    
                    System.out.println("******************************************");

                    //POR ÚLTIMO, METEMOS LOS DATOS EN MONGO
                    //NOS CONECTAMOS A MONGO
                    MongoClient mongoClient = new MongoClient("localhost", 27017);

                    //NOS CONECTAMOS A UNA BD
                    MongoDatabase database = mongoClient.getDatabase("test");

                    //se supone que no nos va a pedir credenciales, así que no los ponemos de momento
                    MongoCollection<Document> collection = database.getCollection("finalveh");
                    
                    Document document = new Document();
                    
                    document.put("_id", id);
                    document.put("dni", dni);
                    document.put("nomec", nomec);
                    document.put("nomv", nomv);
                    
                    if (ncompras.intValue() != 0) {
                        
                        pf = (prezoorixe.doubleValue() - ((2019 - anomatricula.doubleValue()) * 500) - 500 + tasas);
                        document.put("pf", pf);
                        
                    } else {
                        pf = (prezoorixe.doubleValue() - ((2019 - anomatricula.doubleValue()) * 500) - 0 + tasas);
                        document.put("pf", pf);
                        
                    }
                    
                    collection.insertOne(document);
                    
                    mongoClient.close();
                    
                }
                
            }
            
            conn.close();
            
        }
        
        em.getTransaction().commit();
        
    }
    
    public static void main(String[] args) throws SQLException {
        
        Pvehiculos3.Ejercicio();
        
    }
    
}
