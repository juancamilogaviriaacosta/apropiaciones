package apropiaciones;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author juan
 */
public class Apropiaciones {

    private Connection con;

    public static void main(String[] args) {
        try {
            Apropiaciones ap = new Apropiaciones();
            ap.calcularApropiaciones();
        } catch (Exception ex) {
            Logger.getLogger(Apropiaciones.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void calcularApropiaciones() throws Exception {
        String url = "";
        String user = "";
        String passwd = "";

        Class.forName("org.postgresql.Driver");
        con = DriverManager.getConnection(url, user, passwd);

        String sql
                = "SELECT tm.paciente_id, tm.primer_pago_id, tm.saldo_real, tm.monto_apropiar98\n"
                + "FROM core.temp_recalculo_apropiacion tm\n"
                + "WHERE tm.saldo_real > 0\n"
                + "AND tm.saldo_real > tm.monto_apropiar98\n"
                + "AND tm.monto_apropiar98 > 100\n"
                + "AND tm.valor_real_pp > 0\n"
                + "AND AND tm.paciente_id  = 1058690\n"
                + "ORDER BY tm.paciente_id";

        Map<Integer, BigDecimal> mapa = new HashMap<>();
        List<Object[]> rl = getResultList(sql, null);
        for (Object[] tmp : rl) {
            Integer pacienteId = (Integer) tmp[0];
            BigInteger primerPagoId = (BigInteger) tmp[1];
            BigDecimal saldoPac = (BigDecimal) tmp[2];
            BigDecimal montoApropiar = (BigDecimal) tmp[3];

            if (!mapa.containsKey(pacienteId)) {
                mapa.put(pacienteId, saldoPac);
            }

            if (mapa.get(pacienteId).subtract(montoApropiar).compareTo(BigDecimal.ZERO) > 0) {
                aplicarApropiacion(pacienteId, primerPagoId, mapa.get(pacienteId));
                mapa.put(pacienteId, saldoPac.subtract(montoApropiar));
            }
        }
    }

    public void aplicarApropiacion(Integer pacienteId, BigInteger primerPagoId, BigDecimal montoApropiar) throws Exception {
        System.out.println("pacienteId " + pacienteId);
        System.out.println("primerPagoId " + primerPagoId);
        System.out.println("montoApropiar " + montoApropiar);
        String sql = "INSERT INTO core.nota_apropiacion () VALUES ()";
        PreparedStatement st = con.prepareStatement(sql);
        st.setInt(0, pacienteId);
        st.setLong(1, primerPagoId.longValue());
        st.setBigDecimal(2, montoApropiar);
        st.executeUpdate();
    }

    public List<Object[]> getResultList(String sql, List<Object> params) throws Exception {
        PreparedStatement st = con.prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.size(); i++) {
                st.setString(i, params.get(i).toString());
            }
        }

        ResultSet rs = st.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        List<Object[]> resp = new ArrayList<>();
        while (rs.next()) {
            Object[] rowData = new Object[rsmd.getColumnCount()];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                rowData[i] = rs.getObject(i);
            }
            resp.add(rowData);
        }
        return resp;
    }
}
