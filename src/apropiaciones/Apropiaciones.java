package apropiaciones;

import java.math.BigDecimal;
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
            ap.calcularApropiaciones(args);
        } catch (Exception ex) {
            Logger.getLogger(Apropiaciones.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void calcularApropiaciones(String[] args) throws Exception {
        String url = args[0];
        String user = args[1];
        String passwd = args[2];

        Class.forName("org.postgresql.Driver");
        con = DriverManager.getConnection(url, user, passwd);

        String sql
                = "SELECT tm.paciente_id, tm.primer_pago_id, tm.saldo_real, tm.monto_apropiar98\n"
                + "FROM core.temp_recalculo_apropiacion tm\n"
                + "WHERE tm.saldo_real > 0\n"
                + "AND tm.saldo_real > tm.monto_apropiar98\n"
                + "AND tm.monto_apropiar98 > 100\n"
                + "AND tm.valor_real_pp > 0\n"
                + "--AND tm.paciente_id  = 1058690\n"
                + "ORDER BY tm.paciente_id";

        Map<Integer, BigDecimal> mapa = new HashMap<>();
        List<Object[]> rl = getResultList(sql, null);
        for (Object[] tmp : rl) {
            Integer pacienteId = (Integer) tmp[0];
            Long primerPagoId = (Long) tmp[1];
            BigDecimal montoApropiar = (BigDecimal) tmp[3];

            if (!mapa.containsKey(pacienteId)) {
                BigDecimal saldoPac = (BigDecimal) tmp[2];
                mapa.put(pacienteId, saldoPac);
            }

            if (mapa.get(pacienteId).subtract(montoApropiar).compareTo(BigDecimal.ZERO) > 0) {
                aplicarApropiacion(pacienteId, primerPagoId, mapa.get(pacienteId), montoApropiar);
                mapa.put(pacienteId, mapa.get(pacienteId).subtract(montoApropiar));
            }
        }
    }

    public void aplicarApropiacion(Integer pacienteId, Long primerPagoId, BigDecimal saldoPac, BigDecimal montoApropiar) throws Exception {
        System.out.println("pacienteId " + pacienteId);
        System.out.println("primerPagoId " + primerPagoId);
        System.out.println("saldoPac " + saldoPac);
        System.out.println("montoApropiar " + montoApropiar);
        System.out.println("");

        String sql = "INSERT INTO core.nota_apropiacion(fecha_modificacion, usuario_modificacion, estado, monto_acreditar, porcentaje, concepto_nota_debito_id, primer_pago_id) VALUES ('2020-03-23 09:00:00', 'sistema', 'A', ?, 9.8, 64, ?)";
        PreparedStatement st = con.prepareStatement(sql);
        st.setBigDecimal(1, montoApropiar);
        st.setLong(2, primerPagoId);
        st.executeUpdate();
        st.close();
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
                rowData[i] = rs.getObject(i+1);
            }
            resp.add(rowData);
        }
        return resp;
    }
}
