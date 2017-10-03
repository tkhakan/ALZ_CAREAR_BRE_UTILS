CREATE OR REPLACE PACKAGE BODY CUSTOMER."ALZ_CAREAR_BRE_UTILS"
IS

  FUNCTION has_quote_same_quarter(p_contract_id IN VARCHAR2) RETURN NUMBER
  IS
      CURSOR curQuarterCode
      IS
        SELECT distinct NVL (IDENTITY_NO, TAX_NUMBER),
               CITY_CODE,
               DISTRICT_CODE,
               QUARTER_CODE,
               X.PRODUCT_ID,
               P.PARTITION_TYPE,
               DA.REFERENCE_CODE
          FROM wip_koc_ocp_car_ear B,
               KOC_CP_ADDRESS_EXT A,
               CP_ADDRESSES C,
               WIP_KOC_OCP_POL_VERSIONS_EXT E,
               WIP_INTERESTED_PARTIES O,
               WIP_IP_LINKS I,
               KOC_CP_PARTNERS_EXT PHE,
               WIP_POLICY_CONTRACTS X,
               WIP_PARTITIONS P,
               DMT_AGENTS DA,
               WIP_POLICY_BASES PB
        WHERE     B.CONTRACT_ID = p_contract_id
               AND B.RISK_ADD_ID = C.ADD_ID
               AND C.ADD_ID = A.ADD_ID
               AND B.CONTRACT_ID = E.CONTRACT_ID
               AND I.CONTRACT_ID = B.CONTRACT_ID
               AND I.ROLE_TYPE = 'INS'
               AND O.CONTRACT_ID = I.CONTRACT_ID
               AND O.IP_NO = I.IP_NO
               AND O.PARTNER_ID = PHE.PART_ID
               AND X.CONTRACT_ID = B.CONTRACT_ID
               AND P.CONTRACT_ID = B.CONTRACT_ID
               AND P.PARTITION_NO = B.PARTITION_NO
               AND PB.AGENT_ROLE = DA.INT_ID
               AND PB.CONTRACT_ID = B.CONTRACT_ID
               and P.partition_no=i.partition_no;


     CURSOR curQueryQuarterCode (
        v_tck_vkn       VARCHAR,
        v_city_code     VARCHAR,
        v_district_code VARCHAR,
        v_quarter_code  VARCHAR,
        v_product_id    NUMBER,
        v_partition_type VARCHAR,
        v_reference_code VARCHAR --partaj
        )
      IS
        SELECT /*+ leading(phe) */  1
        FROM koc_cp_partners_ext phe,
            ocq_interested_parties o,
            ocq_ip_links i,
            ocq_koc_ocp_buildings b,
            koc_cp_address_ext a,
            cp_addresses c,
            ocq_quotes d,
            ocq_policy_bases z,
            ocq_partitions p,
            dmt_agents da
        WHERE (identity_no = v_tck_vkn or tax_number = v_tck_vkn)
        AND phe.part_id = o.partner_id
        AND i.role_type = 'INS'
          and o.quote_id = i.quote_id
          AND o.ip_no = i.ip_no
          AND b.risk_add_id = c.add_id
          AND c.add_id = a.add_id
          AND i.quote_id = b.quote_id
          AND d.contract_id <> p_contract_id
          AND D.QUOTE_ID = O.QUOTE_ID
          AND NOT EXISTS (SELECT 1 FROM ocp_policy_contracts WHERE contract_id = d.contract_id)
          AND city_code = v_city_code
          AND district_code = v_district_code
          AND quarter_code = v_quarter_code
          AND d.product_id IN (41,42) --INSAAT, MONTAJ
          and p.quote_id = b.quote_id
          AND p.partition_no = b.partition_no
          AND p.partition_type = v_partition_type
          AND da.reference_code <> v_reference_code
          AND z.agent_role = da.int_id
          AND z.quote_id = b.quote_id
          AND d.quote_date > sysdate -15;

      v_type_tcvkn      VARCHAR2 (30);
      v_quarter_code    VARCHAR2 (30);
      v_district_code   VARCHAR2 (30);
      v_city_code       VARCHAR2 (30);
      v_product_id      NUMBER;
      v_partition_type  VARCHAR2 (30);
      v_found           NUMBER := 0;
      v_reference_code  VARCHAR (30); --partaj

  BEGIN

         OPEN curQuarterCode;

         LOOP
              FETCH curQuarterCode INTO v_type_tcvkn, v_city_code, v_district_code, v_quarter_code, v_product_id, v_partition_type, v_reference_code;

              EXIT WHEN curQuarterCode%NOTFOUND;

              IF v_type_tcvkn IS NOT NULL AND v_quarter_code IS NOT NULL
              THEN

                 OPEN curQueryQuarterCode (v_type_tcvkn, v_city_code, v_district_code, v_quarter_code, v_product_id, v_partition_type, v_reference_code) ;
                 FETCH curQueryQuarterCode INTO v_found;
                 CLOSE curQueryQuarterCode;

              END IF;

              EXIT WHEN nvl(v_found,0) = 1;

         END LOOP;

         CLOSE curQuarterCode;

    RETURN nvl(v_found,0);
  END;

  FUNCTION has_multi_pol_same_address(p_contract_id   IN VARCHAR2, -- eski adi: has_multi_pol_same_quarter
                                      p_partition_no    IN NUMBER,
                                      p_agent_role       NUMBER,
                                      p_partner_type   IN VARCHAR2)
      RETURN NUMBER
  IS

  CURSOR curAddress
      IS
        SELECT identity_no ,
               tax_number ,
               a.city_code,
               a.district_code
          FROM wip_koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               wip_koc_ocp_pol_versions_ext e,
               wip_interested_parties o,
               wip_ip_links i,
               koc_cp_partners_ext phe,
               wip_policy_contracts x,
               wip_partitions p
         WHERE     b.contract_id = p_contract_id
               AND b.partition_no = p_partition_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND b.contract_id = e.contract_id
               AND i.contract_id = b.contract_id
               AND i.role_type = p_partner_type
               AND o.contract_id = i.contract_id
               AND o.ip_no = i.ip_no
               AND o.partner_id = phe.part_id
               AND x.contract_id = b.contract_id
               AND p.contract_id = b.contract_id
               AND p.partition_type IN (90,91)
               AND p.partition_no = b.partition_no;

      CURSOR curWip (
        v_tckn        VARCHAR2,
        v_vkn         VARCHAR2,
        v_city_code      VARCHAR2,
        v_district_code  VARCHAR2
        )
      IS
        SELECT 1
          FROM koc_cp_partners_ext phe,
               wip_interested_parties o,
               wip_ip_links i,
               wip_koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               wip_policy_contracts d,
               wip_policy_versions v,
               wip_koc_ocp_pol_versions_ext ve,
               wip_policy_bases z,
               wip_partitions p
         WHERE     ( identity_no = nvl (v_tckn,'-1000' ) or  tax_number = nvl(v_vkn,'-1000') )
               AND phe.PART_ID = o.partner_id
               AND i.role_type = p_partner_type
            --   AND z.agent_role <> p_agent_role
               AND b.contract_id <> p_contract_id
               AND o.contract_id = i.contract_id
               AND o.ip_no = i.ip_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND i.contract_id = b.contract_id
               AND d.contract_id = b.contract_id
               AND city_code = v_city_code
               AND district_code = v_district_code
               AND ve.signature_date >= TRUNC (SYSDATE) - 180
               AND p.contract_id = b.contract_id
               AND NVL(p.version_no , 0) = NVL(b.version_no, 0)
               AND v.contract_id = ve.contract_id
               AND NVL(v.version_no, 0) = NVL(ve.version_no , 0)
               AND p.partition_no = b.partition_no
               AND p.partition_type IN (90,91)
               AND v.top_indicator = 'Y'
               AND b.contract_id = v.contract_id
               AND z.contract_id = v.contract_id ;

      CURSOR curOcp (
        v_tckn        VARCHAR2,
        v_vkn         VARCHAR2,
        v_city_code      VARCHAR2,
        v_district_code  VARCHAR2
        )
      IS
        SELECT 1
          FROM koc_cp_partners_ext phe,
               ocp_interested_parties o,
               ocp_ip_links i,
               koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               ocp_policy_contracts d,
               ocp_policy_versions v,
               koc_ocp_pol_versions_ext ve,
               ocp_policy_bases z,
               ocp_partitions p
         WHERE    ( identity_no = nvl (v_tckn,'-1000' ) or  tax_number = nvl(v_vkn,'-1000') )
               AND phe.PART_ID = o.partner_id
               AND i.role_type = p_partner_type
          --     AND z.agent_role <> p_agent_role
               AND b.contract_id <> p_contract_id
               AND o.contract_id = i.contract_id
               AND o.ip_no = i.ip_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND i.contract_id = b.contract_id
               AND d.contract_id = b.contract_id
               AND city_code = v_city_code
               AND district_code = v_district_code
               AND ve.signature_date >= TRUNC (SYSDATE) - 180
               AND p.contract_id = b.contract_id
               AND NVL(p.version_no ,0) = NVL(b.version_no , 0)
               AND v.contract_id = ve.contract_id
               AND NVL(v.version_no, 0) = NVL(ve.version_no , 0)
               AND p.partition_no = b.partition_no
               AND p.partition_type IN (90,91)
               AND v.top_indicator = 'Y'
               AND b.contract_id = v.contract_id
               AND z.contract_id = v.contract_id  ;

      CURSOR curOcq (
        v_tckn        VARCHAR2,
        v_vkn         VARCHAR2,
        v_city_code      VARCHAR2,
        v_district_code  VARCHAR2
        )
      IS
        SELECT 1
          FROM koc_cp_partners_ext phe,
               ocq_interested_parties o,
               ocq_ip_links i,
               ocq_koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               ocq_koc_ocp_pol_contracts_ext  d ,
               ocq_koc_ocp_pol_versions_ext ve,
               ocq_policy_bases z,
               ocq_quotes q,
               ocq_partitions p
         WHERE     ( identity_no = nvl (v_tckn,'-1000' ) or  tax_number = nvl(v_vkn,'-1000') )
               AND phe.PART_ID = o.partner_id
               AND i.role_type = p_partner_type
          --     AND z.agent_role <> p_agent_role
               AND b.contract_id <> p_contract_id
               AND o.QUOTE_ID = i.QUOTE_ID
               AND o.ip_no = i.ip_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND i.QUOTE_ID = b.QUOTE_ID
               AND b.contract_id <> p_contract_id
               AND d.QUOTE_ID = b.QUOTE_ID
               AND city_code = v_city_code
               AND district_code = v_district_code
               AND ve.signature_date >= TRUNC (SYSDATE) - 180
               AND p.QUOTE_ID = b.QUOTE_ID
               AND NVL(p.version_no,0) = NVL(b.version_no, 0)
               AND z.QUOTE_ID = ve.QUOTE_ID
               AND p.partition_no = b.partition_no
               AND p.partition_type IN (90,91)
               AND b.QUOTE_ID = z.QUOTE_ID
               AND q.QUOTE_ID = z.QUOTE_ID ;


      v_tckn         VARCHAR2 (30);
      v_vkn          VARCHAR2 (30);
      v_district_code   VARCHAR2 (30);
      v_city_code       VARCHAR2 (30);
      v_found           NUMBER := 0;

      BEGIN

         OPEN  curAddress;
         FETCH curAddress INTO v_tckn, v_vkn, v_city_code, v_district_code ;
         CLOSE curAddress;


         IF (v_tckn IS NOT NULL or v_vkn is not null) AND v_city_code IS NOT NULL
         THEN

                 OPEN  curWip (v_tckn, v_vkn, v_city_code, v_district_code) ;
                 FETCH curWip INTO v_found;
                 CLOSE curWip;

             IF v_found <> 1
             THEN
                 OPEN  curOcp (v_tckn, v_vkn, v_city_code, v_district_code) ;
                 FETCH curOcp INTO v_found;
                 CLOSE curOcp;
             END IF ;

             IF v_found <> 1
             THEN
                 OPEN  curOcq (v_tckn, v_vkn, v_city_code, v_district_code) ;
                 FETCH curOcq INTO v_found;
                 CLOSE curOcq;
             END IF ;

         END IF;


        RETURN nvl(v_found,0);
      END;



FUNCTION has_multiple_policy (p_wip_ocp_ocq   IN VARCHAR2,
                                 p_identity_no   IN VARCHAR2,
                                 p_tax_number    IN VARCHAR2,
                                 p_agent_role       NUMBER,
                                 p_partner_type   IN VARCHAR2)
      RETURN NUMBER
   IS
      CURSOR curOcp (
         p_insured_type_tcvkn                 VARCHAR2,
         p_agent_role                         NUMBER
      )
      IS
         SELECT   agent_role, d.contract_id
           FROM   koc_mv_policies_asat d
           WHERE  d.insured_type_tcvkn = p_insured_type_tcvkn
                  AND agent_role <> p_agent_role
                  AND d.branch_ext_ref IN ('4', '7', '3')
                  AND d.tanzim_tarihi >= trunc(sysdate) - 180
                  AND product_id in(41 , 42 ) ;


      CURSOR curWip (
         p_identity_no                 VARCHAR2,
         p_tax_number                  VARCHAR2,
         p_agent_role                  NUMBER
      )
      IS
        select   agent_role
          from   koc_cp_partners_ext phe,
                 wip_interested_parties i,
                 wip_policy_bases c,
                 wip_ip_links ip,
                 wip_koc_ocp_pol_versions_ext cc,
                 wip_koc_ocp_pol_contracts_ext d
        where        p_identity_no is not null
                 and phe.identity_no <> 0
                 and phe.identity_no = p_identity_no
                 and i.partner_id = phe.part_id
                 and nvl(i.version_no,0) = 0
                 and i.contract_id = d.contract_id
                 and cc.contract_id = d.contract_id
                 and c.contract_id = cc.contract_id
                 and c.agent_role <> p_agent_role
                 and ip.contract_id = c.contract_id
                 and ip.ip_no = i.ip_no
                 and ip.role_type = p_partner_type
                 and cc.signature_date >= trunc (sysdate) - 180
                 and nvl (cc.version_no, 0) = 0
                 and d.branch_ext_ref in ('4', '7', '3')
        union all
        select agent_role
          from   koc_cp_partners_ext phe,
                 wip_interested_parties i,
                 wip_policy_bases c,
                 wip_ip_links ip,
                 wip_koc_ocp_pol_versions_ext cc,
                 wip_koc_ocp_pol_contracts_ext d
        where        p_tax_number is not null
                 and phe.tax_number <> 0
                 and phe.tax_number = p_tax_number
                 and i.partner_id = phe.part_id
                 and nvl(i.version_no,0)=0
                 and i.contract_id = d.contract_id
                 and cc.contract_id = d.contract_id
                 and c.contract_id = cc.contract_id
                 and c.agent_role <> p_agent_role
                 and ip.contract_id = c.contract_id
                 and ip.ip_no = i.ip_no
                 and ip.role_type = p_partner_type
                 and cc.signature_date >= trunc (sysdate) - 180
                 and nvl (cc.version_no, 0) = 0
                 and d.branch_ext_ref in ('4', '7', '3');



      CURSOR curOcq (
         p_identity_no                 VARCHAR2,
         p_tax_number                  VARCHAR2,
         p_agent_role                  NUMBER
      )
      IS
         SELECT   agent_role
           FROM   koc_cp_partners_ext phe,
                  ocq_interested_parties i,
                  ocq_ip_links ip,
                  ocq_quotes cc,
                  ocq_policy_bases c,
                  ocq_koc_ocp_pol_contracts_ext d
          WHERE       p_identity_no IS NOT NULL
                  AND phe.identity_no <> 0
                  AND phe.identity_no = p_identity_no
                  AND i.partner_id = phe.part_id
                  AND c.quote_id = i.quote_id
                  AND c.contract_id = i.contract_id
                  AND c.agent_role <> p_agent_role
                  AND ip.quote_id = i.quote_id
                  AND ip.contract_id = i.contract_id
                  AND ip.ip_no = i.ip_no
                  AND nvl(ip.version_no,0) = nvl(i.version_no,0)
                  AND ip.role_type = p_partner_type
                  AND cc.quote_id = ip.quote_id
                  AND cc.contract_id = i.contract_id
                  AND cc.effective_date >= TRUNC (SYSDATE) - 180
                  AND d.quote_id = c.quote_id
                  AND d.contract_id = c.contract_id
                  AND d.branch_ext_ref IN ('4', '7', '3')
         UNION ALL
         SELECT   agent_role
           FROM   koc_cp_partners_ext phe,
                  ocq_interested_parties i,
                  ocq_ip_links ip,
                  ocq_quotes cc,
                  ocq_policy_bases c,
                  ocq_koc_ocp_pol_contracts_ext d
          WHERE       p_tax_number IS NOT NULL
                  AND phe.tax_number <> 0
                  AND phe.tax_number = p_tax_number
                  AND i.partner_id = phe.part_id
                  AND c.quote_id = i.quote_id
                  AND c.contract_id = i.contract_id
                  AND c.agent_role <> p_agent_role
                  AND ip.quote_id = i.quote_id
                  AND ip.contract_id = i.contract_id
                  AND ip.ip_no = i.ip_no
                  AND nvl(ip.version_no,0) = nvl(i.version_no,0)
                  AND ip.role_type = p_partner_type
                  AND cc.quote_id = ip.quote_id
                  AND cc.contract_id = i.contract_id
                  AND cc.effective_date >= TRUNC (SYSDATE) - 180
                  AND d.quote_id = c.quote_id
                  AND d.contract_id = c.contract_id
                  AND d.branch_ext_ref IN ('4', '7', '3');

      recOcp                 curOcp%ROWTYPE;
      recWip                 curWip%ROWTYPE;
      recOcq                 curOcq%ROWTYPE;

      v_insured_type_tcvkn   VARCHAR2 (30);
      v_found                NUMBER := 0;
      v_agent_role           NUMBER := 0;
   BEGIN
      -------OCP------

      IF p_wip_ocp_ocq = 'OCP'
      THEN
         IF (p_identity_no IS NOT NULL AND p_identity_no <> 0)
         THEN
            v_insured_type_tcvkn := 'P' || p_identity_no;
         ELSE
            IF (p_tax_number IS NOT NULL AND p_tax_number <> 0)
            THEN
               v_insured_type_tcvkn := 'I' || p_tax_number;
            END IF;
         END IF;

         recOcp := NULL;
         v_found := 0;
         v_agent_role :=0;

         OPEN curOcp (v_insured_type_tcvkn, p_agent_role);
         FETCH curOcp INTO recOcp;
         CLOSE curOcp;

         v_agent_role := NVL (recOcp.agent_role, 0);

         recOcp := NULL;
         IF v_agent_role <> 0
         THEN
            /* eu 24112015 ikinci defa neyin kontrol edildigi anlasilamadi, 2 farkli acente yerine 3 farkli acente bulmaya calisiyor
            OPEN curOcp (v_insured_type_tcvkn, v_agent_role);
            FETCH curOcp INTO recOcp;
            CLOSE curOcp;

            v_agent_role := NVL (recOcp.agent_role, 0);

            IF v_agent_role <> 0   THEN
               v_found := 1;
            END IF;
            */
             v_found := 1;
         END IF;

         RETURN (v_found);
      END IF;

      ---- WIP ---------

      IF p_wip_ocp_ocq = 'WIP'
      THEN

         recWip := NULL;
         v_found := 0;
         v_agent_role :=0;

         OPEN curWip (p_identity_no, p_tax_number, p_agent_role);
         FETCH curWip INTO recWip;
         CLOSE curWip;

         v_agent_role := NVL (recWip.agent_role, 0);

         recWip := NULL;
         IF v_agent_role <> 0
         THEN
             v_found := 1;
            /* eu 24112015 ikinci defa neyin kontrol edildigi anlasilamadi, 2 farkli acente yerine 3 farkli acente bulmaya calisiyor
            OPEN curWip (p_identity_no, p_tax_number, v_agent_role);
            FETCH curWip INTO recWip;
            CLOSE curWip;

            v_agent_role := NVL (recWip.agent_role, 0);

            IF v_agent_role <> 0   THEN
               v_found := 1;
            END IF;
            */
         END IF;
         RETURN (v_found);
      END IF;

      ------ OCQ -------------------

      IF p_wip_ocp_ocq = 'OCQ'
      THEN
         recOcq := NULL;
         v_found := 0;
         v_agent_role :=0;

         OPEN curOcq (p_identity_no, p_tax_number, p_agent_role);
         FETCH curOcq INTO recOcq;
         CLOSE curOcq;

         v_agent_role := NVL (recOcq.agent_role, 0);

         recOcq := NULL;
         IF v_agent_role <> 0
         THEN
             v_found := 1;
            /* eu 24112015 ikinci defa neyin kontrol edildigi anlasilamadi, 2 farkli acente yerine 3 farkli acente bulmaya calisiyor
            OPEN curOcq (p_identity_no, p_tax_number, v_agent_role);
            FETCH curOcq INTO recOcq;
            CLOSE curOcq;

            v_agent_role := NVL (recOcq.agent_role, 0);

            IF v_agent_role <> 0   THEN
               v_found := 1;
            END IF;
            */
         END IF;
         RETURN (v_found);
      END IF;
    RETURN (0);
   END;

    PROCEDURE multiplepolicy (p_identity_no       VARCHAR2,
                             p_vkn_no            VARCHAR2,
                             p_agent_role        NUMBER,
                             p_partner_type       VARCHAR2,
                             p_found         OUT NUMBER)
   IS
      i               NUMBER;
      v_agent         VARCHAR2 (20);
      v_found         NUMBER;
      v_found_agent   NUMBER;
   BEGIN
      i := 0;
      v_agent := '';
      v_found := 0;

      IF NVL (p_identity_no, 'X') <> 'X'
      THEN
         v_found := has_multiple_policy ('OCP',p_identity_no,  NULL, p_agent_role , p_partner_type );
         IF v_found <> 1
         THEN
            v_found := has_multiple_policy ('WIP',p_identity_no,  NULL, p_agent_role , p_partner_type );
         END IF;
         IF v_found <> 1
         THEN
            v_found := has_multiple_policy ('OCQ',p_identity_no,  NULL, p_agent_role , p_partner_type );
         END IF;
      END IF;



      IF NVL (p_vkn_no, 'X') <> 'X'  and v_found <> 1
      THEN
         v_found :=has_multiple_policy ('OCP', NULL,p_vkn_no, p_agent_role , p_partner_type );
         IF v_found <> 1
         THEN
            v_found :=has_multiple_policy ('WIP', NULL,p_vkn_no, p_agent_role , p_partner_type );
         END IF;
         IF v_found <> 1
         THEN
            v_found :=has_multiple_policy ('OCQ', NULL,p_vkn_no, p_agent_role , p_partner_type );
         END IF;
      END IF;

      p_found := v_found;
   END;

--   PROCEDURE multiplepolicy (p_identity_no       VARCHAR2,
--                             p_vkn_no            VARCHAR2,
--                             p_found         OUT NUMBER) is
--   begin
--     multiplepolicy (p_identity_no, p_vkn_no, 0, p_found);
--   end;


    FUNCTION has_multiple_factor (p_wip_ocp_ocq    IN VARCHAR2,
                                  p_part_id        IN NUMBER,
                                  p_agent_role        NUMBER,
                                  p_partner_type   IN VARCHAR2)
        RETURN NUMBER
    IS
        CURSOR curOcp (
             p_part_id                         NUMBER,
             p_agent_role                      NUMBER
        )
        IS

                 SELECT   agent_role
            FROM     koc_cp_partners_ext phe,
                     ocp_interested_parties i,
                     ocp_policy_bases c,
                     ocp_ip_links ip,
                     koc_ocp_pol_versions_ext cc,
                     koc_ocp_pol_contracts_ext d
            WHERE    phe.part_id=p_part_id
                     AND i.partner_id = phe.part_id
                     AND nvl(i.version_no,0)=0
                     AND i.contract_id = d.contract_id
                     AND cc.contract_id = d.contract_id
                     AND c.contract_id = cc.contract_id
                     AND c.agent_role <> p_agent_role
                     AND ip.contract_id = c.contract_id
                     AND ip.ip_no = i.ip_no
                     AND ip.role_type = p_partner_type
                     AND nvl (cc.version_no, 0) = 0
                     AND d.branch_ext_ref in ('4', '7', '3');


        CURSOR curWip (
            p_part_id                         NUMBER,
            p_agent_role                      NUMBER
        )
        IS


            SELECT   agent_role
            FROM     koc_cp_partners_ext phe,
                     wip_interested_parties i,
                     wip_policy_bases c,
                     wip_ip_links ip,
                     wip_koc_ocp_pol_versions_ext cc,
                     wip_koc_ocp_pol_contracts_ext d
            WHERE    phe.part_id=p_part_id
                     AND i.partner_id = phe.part_id
                     AND nvl(i.version_no,0)=0
                     AND i.contract_id = d.contract_id
                     AND cc.contract_id = d.contract_id
                     AND c.contract_id = cc.contract_id
                     AND c.agent_role <> p_agent_role
                     AND ip.contract_id = c.contract_id
                     AND ip.ip_no = i.ip_no
                     AND ip.role_type = p_partner_type
                     AND nvl (cc.version_no, 0) = 0
                     AND d.branch_ext_ref in ('4', '7', '3');



        CURSOR curOcq (
            p_part_id                         NUMBER,
            p_agent_role                      NUMBER
        )
        IS

            SELECT   agent_role
            FROM     koc_cp_partners_ext phe,
                     ocq_interested_parties i,
                     ocq_ip_links ip,
                     ocq_quotes cc,
                     ocq_policy_bases c,
                     ocq_koc_ocp_pol_contracts_ext d
            WHERE    phe.part_id=p_part_id
                     AND i.partner_id = phe.part_id
                     AND c.quote_id = i.quote_id
                     AND c.contract_id = i.contract_id
                     AND c.agent_role <> p_agent_role
                     AND ip.quote_id = i.quote_id
                     AND ip.contract_id = i.contract_id
                     AND ip.ip_no = i.ip_no
                     AND nvl(ip.version_no,0) = nvl(i.version_no,0)
                     AND ip.role_type = p_partner_type
                     AND cc.quote_id = ip.quote_id
                     AND cc.contract_id = i.contract_id
                     AND d.quote_id = c.quote_id
                     AND d.contract_id = c.contract_id
                     AND d.branch_ext_ref IN ('4', '7', '3');

        recOcp                 curOcp%ROWTYPE;
        recWip                 curWip%ROWTYPE;
        recOcq                 curOcq%ROWTYPE;
        v_found_factor                NUMBER := 0;
        v_found                NUMBER := 0;
        v_agent_role           NUMBER := 0;

   BEGIN
      -------OCP------

      IF p_wip_ocp_ocq = 'OCP'
      THEN

         recOcp := NULL;
         v_found_factor := 0;
         v_agent_role :=0;

         OPEN curOcp (p_part_id, p_agent_role);
         FETCH curOcp INTO recOcp;
         CLOSE curOcp;

         v_agent_role := NVL (recOcp.agent_role, 0);

         recOcp := NULL;
         IF v_agent_role <> 0
         THEN
             v_found_factor := 1;
         END IF;

         RETURN (v_found_factor);
      END IF;

      ---- WIP ---------

      IF p_wip_ocp_ocq = 'WIP'
      THEN

         recWip := NULL;
         v_found_factor := 0;
         v_agent_role :=0;

         OPEN curWip (p_part_id, p_agent_role);
         FETCH curWip INTO recWip;
         CLOSE curWip;

         v_agent_role := NVL (recWip.agent_role, 0);

         recWip := NULL;
         IF v_agent_role <> 0
         THEN
             v_found_factor := 1;
         END IF;
         RETURN (v_found_factor);
      END IF;

      ------ OCQ -------------------

      IF p_wip_ocp_ocq = 'OCQ'
      THEN
         recOcq := NULL;
         v_found_factor := 0;
         v_agent_role :=0;

         OPEN curOcq (p_part_id, p_agent_role);
         FETCH curOcq INTO recOcq;
         CLOSE curOcq;

         v_agent_role := NVL (recOcq.agent_role, 0);

         recOcq := NULL;
         IF v_agent_role <> 0
         THEN
             v_found_factor := 1;

         END IF;
         RETURN (v_found_factor);
      END IF;
    RETURN (0);
   END;

    FUNCTION bre_find_old_contract_id (p_contract_id    IN NUMBER)

        RETURN NUMBER
    IS
        CURSOR c_old_pol_contract
        IS
            SELECT   x.old_contract_id,x.REFERENCE_CONTRACT_ID,x.OIP_POLICY_REF
            FROM     wip_koc_ocp_pol_contracts_ext x
            WHERE    contract_id = p_contract_id
        UNION
            SELECT   x.old_contract_id,x.REFERENCE_CONTRACT_ID,x.OIP_POLICY_REF
            FROM     koc_ocp_pol_contracts_ext x
            WHERE    contract_id = p_contract_id;

    p_old_contract_id           NUMBER;
    p_reference_contract_id     NUMBER;
    p_oip_policy_ref            wip_koc_ocp_pol_contracts_ext.OIP_POLICY_REF%type;

        CURSOR c_yk_old_contract(
            c_ref_contract_id                 NUMBER
        )
        IS
            SELECT old_contract_id
            FROM   koc_ocp_pol_contracts_ext
            WHERE      old_contract_id IS NOT NULL
                   AND reference_contract_id IS NULL
                   START WITH contract_id = c_ref_contract_id
                   CONNECT BY PRIOR reference_contract_id=contract_id;


        CURSOR c_oip_old_contract(
            c_oip_contract_id                 NUMBER
        )
        IS
            SELECT old_contract_id
            FROM   koc_ocp_pol_contracts_ext a
            WHERE      old_contract_id IS NOT NULL
                   AND block_policy_no IS NULL
                   START WITH a.contract_id = c_oip_contract_id
                   CONNECT BY PRIOR block_policy_no=contract_id;

        CURSOR c_yks_old_policy_ref
        IS
            SELECT prev_ins_comp_policy_ref
            FROM   koc_ocp_partitions_ext a
            WHERE      prev_insurance_comp_code = 2681589 --YKS
                   AND contract_id = p_contract_id
        UNION
            SELECT prev_ins_comp_policy_ref
            FROM   wip_koc_ocp_partitions_ext a
            WHERE      prev_insurance_comp_code = 2681589 --YKS
                   AND contract_id = p_contract_id;

        CURSOR c_contract_id(
            c_policy_ref                       VARCHAR2
        )
        IS
            SELECT contract_id
            FROM   ocp_policy_bases
            WHERE  policy_ref = c_policy_ref;

     v_old_contract_id          NUMBER;
     v_oip_contract_id          NUMBER;
     v_yks_old_policy_ref       VARCHAR2(20);

    BEGIN
         open  c_old_pol_contract;
         fetch c_old_pol_contract into p_old_contract_id,p_reference_contract_id,p_oip_policy_ref;
         close c_old_pol_contract;

         if  p_old_contract_id is not null then

              v_old_contract_id := p_old_contract_id;

         elsif p_reference_contract_id is not null then

               open c_yk_old_contract(p_reference_contract_id);
               fetch c_yk_old_contract into v_old_contract_id;
               if c_yk_old_contract%notfound then
                  v_old_contract_id := null;
               end if;
               close c_yk_old_contract;

         elsif p_oip_policy_ref is not null then
               open  c_contract_id(p_oip_policy_ref );
               fetch c_contract_id into v_oip_contract_id;
               close c_contract_id;

               open c_oip_old_contract(v_oip_contract_id);
               fetch c_oip_old_contract into v_old_contract_id;
               if c_oip_old_contract%notfound then
                  v_old_contract_id := null;
               end if;
               close c_oip_old_contract;
         else

            open c_yks_old_policy_ref;
            fetch c_yks_old_policy_ref into v_yks_old_policy_ref;
            close c_yks_old_policy_ref;

            if v_yks_old_policy_ref is not null then
               return (1);

            end if;
         end if;

        return(v_old_contract_id) ;

    END;

   PROCEDURE multiplefactorpolicy (p_part_id            NUMBER,
                             p_agent_role         NUMBER,
                             p_partner_type       VARCHAR2,
                             p_found_factor          OUT NUMBER)
   IS
      i               NUMBER;
      v_agent         VARCHAR2(20);
      v_found_factor         NUMBER;
      v_found_agent   NUMBER;
   BEGIN
      i := 0;
      v_agent := '';
      v_found_factor := 0;

         v_found_factor := has_multiple_factor ('OCP',p_part_id, p_agent_role, p_partner_type);
         IF v_found_factor <> 1
         THEN
            v_found_factor := has_multiple_factor ('WIP',p_part_id, p_agent_role, p_partner_type);
         END IF;
         IF v_found_factor <> 1
         THEN
            v_found_factor := has_multiple_factor ('OCQ',p_part_id, p_agent_role, p_partner_type);
         END IF;




      p_found_factor := v_found_factor;
   END;

   PROCEDURE multiplefactorpolicy (p_part_id            NUMBER,
                             p_found_factor           OUT NUMBER) is
   begin
     multiplefactorpolicy (p_part_id, 0,'INS', p_found_factor);
   end;


   PROCEDURE p_claim_info (p_contract_id           NUMBER,
                           p_claims            OUT CAREAR_BRE_CLAIM_TABLE)
   IS
      v_partition_no       NUMBER (5);
      v_status_id          NUMBER (10);
      v_skip               INT;
      v_losscause          VARCHAR2 (10);
      v_paid               NUMBER;
      v_reserveloss        NUMBER;
      v_can_be_recourse    INT;
      v_version_no         NUMBER (5);
      v_count              INT;
      v_count2             INT;
      p_found_factor              INT;
      v_process_results    CUSTOMER.PROCESS_RESULT_TABLE;

      CURSOR adress_coordinates (
          p_partition_no    IN NUMBER
      )
      IS
          SELECT a.xcoor, a.ycoor
          FROM   bv_koc_ocp_buildings b,
                 koc_cp_address_ext a,
                 cp_addresses c,
                 bv_koc_ocp_pol_versions_ext e
          WHERE      b.contract_id = p_contract_id
                 AND b.partition_no = p_partition_no
                 AND b.risk_add_id = c.add_id
                 AND c.add_id = a.add_id
                 AND b.contract_id = e.contract_id;

      coordinates          adress_coordinates%ROWTYPE;
      v_coordinate_found   NUMBER;
      v_signature_date     DATE;

      CURSOR earthquake_dates (
         p_xcoor            IN NUMBER,
         p_ycoor            IN NUMBER
      )
      IS
          SELECT r.validity_start_date, r.validity_end_date
          FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE      a.shape_id = r.shape_id
                 AND r.risk_type = 2 --is kabul haddi
                 AND r.dune_type = 2 --deprem riski
                 AND r.auth_type = 2 --otorizasyona duser
                 AND a.validity_start_date <= sysdate
                 AND a.validity_end_date >= sysdate
                 AND r.validity_start_date <= sysdate
                 AND r.validity_end_date >= sysdate
                 AND sdo_relate(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

                  /* Performans arttirma amacli SDO_GEOM.RELATE yerine
                     SDO_RELATE kullaniyoruz,tum cografi risk tanimlari icin
                     yukaridaki sekilde degistirildi --eu
                  AND (sdo_geom.relate (
                          a.coordinates,
                          'anyinteract',
                          (SDO_GEOMETRY (
                              2001,
                              NULL,
                              sdo_point_type (NVL (p_xcoor, 0),
                                              NVL (p_ycoor, 0),
                                              NULL),
                              NULL,
                              NULL
                           )),
                          v_flexibility
                       ) = 'TRUE'); */


      earthquakedates      earthquake_dates%ROWTYPE;
      v_default_end_date   DATE;

      CURSOR flood_dates (
         p_xcoor             IN NUMBER,
         p_ycoor             IN NUMBER
      )
      IS
          SELECT r.validity_start_date, r.validity_end_date
          FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE      a.shape_id = r.shape_id
                 AND r.risk_type = 2 --is kabul haddi
                 AND r.dune_type = 4 --sel riski
                 AND r.auth_type = 2 --otorizasyona duser
                 AND a.validity_start_date <= sysdate
                 AND a.validity_end_date >= sysdate
                 AND r.validity_start_date <= sysdate
                 AND r.validity_end_date >= sysdate
                 AND sdo_relate(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      flooddates           flood_dates%ROWTYPE;

      CURSOR landslide_dates (
         p_xcoor             IN NUMBER,
         p_ycoor             IN NUMBER
      )
      IS
          SELECT r.validity_start_date, r.validity_end_date
          FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE      a.shape_id = r.shape_id
                 AND r.risk_type = 2 --is kabul haddi
                 AND r.dune_type = 6 --yer kaymasi riski
                 AND r.auth_type = 2 --otorizasyona duser
                 AND a.validity_start_date <= sysdate
                 AND a.validity_end_date >= sysdate
                 AND r.validity_start_date <= sysdate
                 AND r.validity_end_date >= sysdate
                 AND sdo_relate(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      landslidedates       landslide_dates%ROWTYPE;

      CURSOR terror_dates (
         p_xcoor              IN NUMBER,
         p_ycoor              IN NUMBER
      )
      IS
          SELECT r.validity_start_date, r.validity_end_date
          FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE      a.shape_id = r.shape_id
                 AND r.risk_type = 2 --is kabul haddi
                 AND r.dune_type = 3 --teror riski
                 AND r.auth_type = 2 --otorizasyona duser
                 AND a.validity_start_date <= sysdate
                 AND a.validity_end_date >= sysdate
                 AND r.validity_start_date <= sysdate
                 AND r.validity_end_date >= sysdate
                 AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      terrordates          terror_dates%ROWTYPE;

      CURSOR c_in_terror_list
      IS
          SELECT 1
          FROM   bv_koc_ocp_buildings b,
                 koc_cp_address_ext a,
                 pme_bv_policy_versions e,
                 alz_terror_control t,
                 pme_bv_partitions p,
                 pme_bv_policy_contracts pc
          WHERE      b.contract_id = p_contract_id
                 AND b.contract_id = p.contract_id
                 AND b.partition_no = v_partition_no
                 AND b.partition_no = p.partition_no
                 AND b.contract_id = pc.contract_id
                 AND b.risk_add_id = a.add_id
                 AND b.contract_id = e.contract_id
                 AND t.partition_type = p.partition_type
                 AND t.product_id = pc.product_id
                 AND t.city_code = a.city_code
                 AND (t.district_code = a.district_code
                      OR NVL(lower(t.district_code), 'x') = 'x')         -- Ilin butun ilcelerinin otorizasyona dusmesi istendiginde
                  --AND t.priority = 1
                  --AND t.is_industrial = 1
                 AND t.validity_start_date < trunc(sysdate)
                 AND (t.validity_end_date > trunc(sysdate)
                  OR t.validity_end_date IS NULL);

      v_in_terror_list NUMBER(1);

      CURSOR c_address (
         p_earthquake_start_date   IN  DATE,
         p_earthquake_end_date     IN  DATE,
         p_flood_start_date        IN  DATE,
         p_flood_end_date          IN  DATE,
         p_landslide_start_date    IN  DATE,
         p_landslide_end_date      OUT DATE,
         p_terror_start_date       IN  DATE,
         p_terror_end_date         IN  DATE
      )
      IS
         SELECT   b.risk_add_id,
                  a.city_code,
                  c.country_code,
                  b.cresta_zone_code,
                  a.district_code,
                  p_earthquake_end_date earthquake_auth_end_date,
                  p_earthquake_start_date earthquake_auth_start_date,
                  b.earthquake_zone_code,
                  p_flood_end_date flood_auth_end_date,
                  p_flood_start_date flood_auth_start_date,
                  p_landslide_end_date landslide_end_date,
                  p_landslide_start_date landslide_start_date,
                  b.municipality_code,
                  a.quarter_code,
                  p_terror_end_date terror_end_date,
                  p_terror_start_date terror_start_date,
                  koc_address_utils.address (b.risk_add_id) adres_txt
           FROM   bv_koc_ocp_buildings b,
                  koc_cp_address_ext a,
                  cp_addresses c,
                  bv_koc_ocp_pol_versions_ext e
          WHERE       b.contract_id = p_contract_id
                  AND b.partition_no = v_partition_no
                  AND b.risk_add_id = c.add_id
                  AND c.add_id = a.add_id
                  AND b.contract_id = e.contract_id;

      reco_address         c_address%ROWTYPE;

      CURSOR find_signature_date
      IS
         SELECT   e.signature_date
           FROM   wip_koc_ocp_pol_versions_ext e
          WHERE   e.contract_id = p_contract_id;

      CURSOR c_partners (
         r_claim_id NUMBER
      )
      IS
         SELECT   cp.part_id,
                  cp.first_name,
                  cp.surname,
                  cp.date_of_birth,
                  cp.nationality,
                  cp.marital_status,
                  kx.identity_no,
                  kx.tax_number,
                  ci.ip_type role_type,
                  DECODE (
                     (SELECT   1
                        FROM   koc_risk_partners
                       WHERE   (tax_number = kx.tax_number
                                OR identity_no = kx.identity_no)
                               AND entry_date <= cb.date_of_loss
                               AND (cancel_date IS NULL
                                    OR cancel_date >= cb.date_of_loss)
                               AND ROWNUM < 2),
                     1,
                     '1',
                     '0'
                  )
                     risky,
                DECODE (
                    (select count(*) from koc_cp_blacklist_entries where part_id in (
                        select part_id from koc_cp_partners_ext
                            where identity_no in (kx.identity_no) or tax_number in (kx.tax_number))
                                AND from_date <= cb.date_of_loss
                                AND (TO_DATE IS NULL
                                 OR TO_DATE >= cb.date_of_loss)
                    ),
                    0,
                    '0',
                    '1'
                 )
                 blacklist,
                 cp.partner_type,
                DECODE (
                    (SELECT count(*) FROM alz_ins_corruption
                        WHERE suspect_identity = kx.identity_no or suspect_tax_number = kx.tax_number
                    ),
                    0,
                    '0',
                    '1'
                 )
                 sisbis,
                 NVL(kx.is_foreign_company, 0) is_foreign_company
           FROM   clm_pol_bases cb,
                  clm_interested_parties ci,
                  cp_partners cp,
                  koc_cp_partners_ext kx
          WHERE       ci.claim_id = r_claim_id
                  AND ci.claim_id = cb.claim_id
                  AND ci.part_id = cp.part_id
                  AND cp.part_id = kx.part_id;

      reco_partners        c_partners%ROWTYPE;
   BEGIN
      v_count := 0;
      p_claims := carear_bre_claim_table ();

      OPEN find_signature_date;
      FETCH find_signature_date INTO v_signature_date;
      CLOSE find_signature_date;

      FOR reco
      IN (SELECT   cs.ext_reference,
                   cs.claim_id,
                   cs.sf_no,
                   cs.sf_type
            FROM   clm_subfiles cs, clm_pol_bases cb
           WHERE       cb.contract_id = p_contract_id
                   AND cb.claim_id = cs.claim_id
                   AND cs.clm_status <> 'CANCELLED')
      LOOP
         SELECT   MAX (status_id)
           INTO   v_status_id
           FROM   clm_status_history
          WHERE       claim_id = reco.claim_id
                  AND sf_no = reco.sf_no
                  AND csh_comment LIKE '%Red%yaz%';

         IF v_status_id IS NOT NULL
         THEN
            SELECT   COUNT ( * )
              INTO   v_skip
              FROM   clm_status_history
             WHERE   status_id = v_status_id
                     AND csh_comment LIKE '%Red yazisi iptal%';

            IF v_skip <> 0
            THEN
               NULL;                                          -- geçerli hasar
            ELSE
               GOTO skipi;
            END IF;
         END IF;

         SELECT   cause1_code
           INTO   v_losscause
           FROM   koc_clm_subfile_cause1_rel
          WHERE       claim_id = reco.claim_id
                  AND sf_no = reco.sf_no
                  AND ROWNUM = 1;

         SELECT   NVL (SUM (DECODE (sf_total_type,
                                    2,
                                    NVL (trans_amt, 0),
                                    3,
                                    NVL (trans_amt, 0),
                                    9,
                                    NVL (trans_amt, 0))), 0)
                  - NVL (SUM (DECODE (sf_total_type, 6, NVL (trans_amt, 0))),
                         0)
           INTO   v_paid
           FROM   koc_clm_bordro_rep
          WHERE       claim_id = reco.claim_id
                  AND sf_no = reco.sf_no ;
               --   AND sf_total_type = 3
               --   AND sf_type <> 'HLT';


        SELECT   ROUND (
                     SUM(NVL (a.trans_amt, 0)
                         * NVL (
                              koc_curr_utils.retrieve_curr_selling (
                                 trans_amt_swf,
                                 LAST_DAY (SYSDATE),
                                 NULL
                              ),
                              1
                           )),
                     find_round_digit (base_swift_code, 'P')
                  )
               INTO   v_reserveloss
          FROM   CLM_TRANS a , CLM_SUBFILES b
         WHERE    a.claim_id = b.claim_id
                  AND a.sf_no = b.sf_no
                  AND a.claim_id = reco.claim_id
                  AND a.sf_no = reco.sf_no
                  AND b.clm_status <> 'CLOSED'           -- claim statüsde dosya kapalý ise muallagý alma
                  AND sf_total_type IN ('10', '9', '99')
                  AND trans_date =
                        (SELECT   MAX (trans_date)
                           FROM   clm_trans b
                          WHERE       a.claim_id = b.claim_id
                                  AND a.sf_no = b.sf_no
                                  AND a.ext_reference = b.ext_reference
                                  AND b.sf_total_type IN ('10', '9', '99')
                                  AND b.trans_date <= LAST_DAY (SYSDATE))
                  AND trans_no =
                        (SELECT   MAX (trans_no)
                           FROM   clm_trans c
                          WHERE       a.claim_id = c.claim_id
                                  AND a.sf_no = c.sf_no
                                  AND a.ext_reference = c.ext_reference
                                  AND sf_total_type IN ('10', '9', '99')
                                  AND a.cover_no = c.cover_no
                                  AND trans_date =
                                        (SELECT   MAX (trans_date)
                                           FROM   clm_trans d
                                          WHERE   a.claim_id = d.claim_id
                                                  AND a.sf_no = d.sf_no
                                                  AND a.ext_reference =
                                                        d.ext_reference
                                                  AND a.cover_no = d.cover_no
                                                  AND sf_total_type IN
                                                           ('10', '9', '99')
                                                  AND d.trans_date <=
                                                        LAST_DAY (SYSDATE)))
                  AND trans_date <= LAST_DAY (SYSDATE);
                -- tazminat_muallak yeni hali


       /*
         SELECT   SUM (NVL (trans_amt, 0))
           INTO   v_reserveloss
           FROM   koc_clm_muallak_bordro_rep
          WHERE       claim_id = reco.claim_id
                  AND sf_no = reco.sf_no
                  AND ticket_date = LAST_DAY (TRUNC (SYSDATE))
                  AND NVL (is_ibnr, 0) <> 1
                  AND trans_amt > 0
                  AND sf_total_type = '10';              */   -- tazminat_muallak

         SELECT   can_be_recourse
           INTO   v_can_be_recourse
           FROM   koc_clm_detail
          WHERE   claim_id = reco.claim_id AND sf_no = reco.sf_no;

         v_count := v_count + 1;
         p_claims.EXTEND;
         p_claims (v_count) :=
            carear_bre_claim (reco.ext_reference,
                           reco.sf_type,
                           v_losscause,
                           reco.claim_id,
                           NVL (v_paid, 0),
                           NVL (v_reserveloss, 0),
                           NULL,            --partners CAREAR_BRE_PARTNER_TABLE ,
                           NVL (v_can_be_recourse, 0),      --rucuRate number,
                           NULL          --basic_address CAREAR_BRE_basic_address
                               );

         BEGIN
            SELECT   oar_no, version_no
              INTO   v_partition_no, v_version_no
              FROM   clm_pol_oar
             WHERE       contract_id = p_contract_id
                     AND claim_id = reco.claim_id
                     AND ROWNUM < 2;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_partition_no := 0;
         END;

         IF v_partition_no = 0
         THEN
            p_claims (v_count).basic_address := NULL;
         ELSE
            pme_api.set_query_version (v_version_no);

            --adres koordinatlari aliniyor
            OPEN adress_coordinates (v_partition_no);
            FETCH adress_coordinates INTO coordinates;
            IF adress_coordinates%NOTFOUND THEN
                coordinates := NULL;
            END IF;
            CLOSE adress_coordinates;

            --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri aliniyor
            IF coordinates.xcoor IS NOT NULL
               AND coordinates.ycoor IS NOT NULL
            THEN
               v_coordinate_found := 1;

               OPEN earthquake_dates (coordinates.xcoor, coordinates.ycoor);
               FETCH earthquake_dates INTO earthquakedates;
               IF earthquake_dates%NOTFOUND THEN
                    earthquakedates := NULL;
               END IF;
               CLOSE earthquake_dates;

               OPEN flood_dates (coordinates.xcoor, coordinates.ycoor);
               FETCH flood_dates INTO flooddates;
               IF flood_dates%NOTFOUND THEN
                    flooddates := NULL;
               END IF;
               CLOSE flood_dates;

               OPEN landslide_dates (coordinates.xcoor, coordinates.ycoor);
               FETCH landslide_dates INTO landslidedates;
               IF landslide_dates%NOTFOUND THEN
                    landslidedates := NULL;
               END IF;
               CLOSE landslide_dates;

               OPEN terror_dates (coordinates.xcoor, coordinates.ycoor);
               FETCH terror_dates INTO terrordates;
               IF (terror_dates%NOTFOUND OR terrordates.validity_end_date < sysdate)THEN
                    OPEN c_in_terror_list;
                    FETCH c_in_terror_list INTO v_in_terror_list;
                    IF c_in_terror_list%NOTFOUND THEN
                      terrordates := NULL;
                    ELSE -- Teror il/ilce listesindeyse start/end date'i otorizasyona dusecek sekilde set et
                      terrordates.validity_start_date := sysdate-2;
                      terrordates.validity_end_date := sysdate+2;
                    END IF;
                    CLOSE c_in_terror_list;
               END IF;
               CLOSE terror_dates;
            END IF;
            --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri alindi

            IF v_coordinate_found = 1
            THEN
               OPEN c_address (earthquakedates.validity_start_date,
                               earthquakedates.validity_end_date,
                               flooddates.validity_start_date,
                               flooddates.validity_end_date,
                               landslidedates.validity_start_date,
                               landslidedates.validity_end_date,
                               terrordates.validity_start_date,
                               terrordates.validity_end_date);
            ELSE
               OPEN c_address (v_signature_date - 1,
                               v_default_end_date,
                               v_signature_date - 1,
                               v_default_end_date,
                               v_signature_date - 1,
                               v_default_end_date,
                               v_signature_date - 1,
                               v_default_end_date);
            END IF;

            FETCH c_address INTO reco_address;

            IF c_address%NOTFOUND
            THEN
               p_claims (v_count).basic_address := NULL;
            ELSE
               p_claims (v_count).basic_address :=
                  CAREAR_BRE_basic_address (reco_address.risk_add_id,
                                         reco_address.country_code,
                                         reco_address.city_code,
                                         reco_address.district_code,
                                         reco_address.quarter_code,
                                         reco_address.adres_txt);
            END IF;

            CLOSE c_address;
         END IF;

         v_count2 := 0;
         p_claims (v_count).partners := CAREAR_BRE_partner_table ();

         OPEN c_partners (reco.claim_id);

         LOOP
            FETCH c_partners INTO reco_partners;

            EXIT WHEN c_partners%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_claims (v_count).partners.EXTEND;
            p_claims (v_count).partners (v_count2) :=
               CAREAR_BRE_partner (reco_partners.part_id,
                                reco_partners.first_name,
                                reco_partners.surname,
                                reco_partners.date_of_birth,
                                reco_partners.nationality,
                                reco_partners.marital_status,
                                reco_partners.identity_no,
                                reco_partners.tax_number,
                                reco_partners.role_type,
                                reco_partners.risky,
                                reco_partners.blacklist,
                                reco_partners.partner_type,
                                NULL,
                                0,
                                0,
                                0,
                                NULL,
                                NULL,
                                NULL,
                                reco_partners.sisbis,
                                0,   --EH Score
                                0,   --previously denied
                                reco_partners.is_foreign_company,
                                0,
                                0);
         END LOOP;

         CLOSE c_partners;

         IF v_count2 = 0
         THEN
            p_claims (v_count).partners := NULL;
         END IF;

        <<skipi>>
         NULL;
      END LOOP;

      IF v_count = 0
      THEN
         p_claims := NULL;
      END IF;

      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_claims := NULL;
         alz_web_process_utils.process_result (
            0,
            9,
            -1,
            'INVALID_DATA',
            'Hasar Tespit Hata',
            'Hasar Tespit Hata',
            NULL,
            NULL,
            'alz_carear_bre_utils , p_claim_info',
            NULL,
            v_process_results
         );
   END p_claim_info;


---wip


PROCEDURE p_wip (p_contract_id           NUMBER,
                 p_wip_policy        OUT CAREAR_BRE_POLICY,
                 p_process_results   OUT CUSTOMER.PROCESS_RESULT_TABLE)
IS
   v_count                   INT;
   v_count2                  INT;
   v_partition_no            NUMBER (5);
   v_contract_id             NUMBER (10);
   v_old_contract_id         NUMBER (10);
   v_version_no              NUMBER (5);
   v_claims                  CAREAR_BRE_CLAIM_TABLE;
   v_found_factor                   NUMBER (1);
   v_found                          NUMBER (1);
   v_quote_same_quarter_found          NUMBER(1);
   v_multi_pol_same_quarter_found      NUMBER(1);

   CURSOR c_policy
   IS
      SELECT   c.oip_policy_ref,
               -- agent
               v.business_start_date,
               b.contract_id,
               (CASE
                   WHEN c.contract_type = 0 AND c.prev_quote_ref IS NULL
                   THEN
                      2
                   WHEN c.contract_type = 0 AND c.prev_quote_ref IS NOT NULL
                   THEN
                      3
                   WHEN c.contract_type = 1
                        AND v.movement_reason_code <> 'ENBQ'
                   THEN
                      0
                   WHEN c.contract_type = 1
                        AND v.movement_reason_code = 'ENBQ'
                   THEN
                      1
                END)
                  contract_type,
               --0=poliçe, 1=teklife istinaden poliçe ,2=teklif, 3=teklife istinaden teklif
               NVL (ve.endorsement_no, 0) endorsement_no,     -- default value
               NVL (
                  koc_curr_utils.retrieve_currency_buying (
                     'EUR',
                     v.business_start_date
                  ),
                  1
               )
                  eur_exchg_rate,                             -- default value
                 NVL (v.full_term_premium, 0)
               + NVL (ve.fire_tax, 0)
               + NVL (ve.insur_process_tax, 0)
                  gross_premium_tl,
                  (  NVL (v.adjustment_premium, 0)
                   + NVL (ve.adj_insur_process_tax, 0)
                   + NVL (ve.adj_traffic_fund, 0)
                   + NVL (ve.adj_guarantee_fund, 0)
                   + NVL (ve.adj_fire_tax, 0))
                 adj_gross_premium_tl,
               ve.group_code,
               NVL (v.full_term_premium, 0) net_premium_tl,  --default value
               -- policy holders
               -- policy partitions
               DECODE (bre_find_old_contract_id (p_contract_id), NULL, '0', '1')
                  policy_type,
               -- prev fire claims
               NVL (c.old_contract_id, 0) old_contract_id,    -- default valie
               v.product_id,
               NVL (c.is_renewal, 0) is_renewal,              -- default value
               NVL (c.reference_contract_id, 0) reference_contract_id, -- default value
               ve.signature_date,
               b.term_end_date,
               b.term_start_date,
               NVL (
                  koc_curr_utils.retrieve_currency_buying (
                     'USD',
                     v.business_start_date
                  ),
                  1
               )
                  usd_exchg_rate,                             -- default value
               --v.username,
               NVL (v.version_no, 0) version_no,              -- default value
               ve2.signature_date pol_signature_date,
               NVL (ve.is_industrial, 0) is_industrial,
               NVL (ve.aff_payment_type, '0') aff_payment_type,
               NVL (ve.is_date_fixed, 0) is_date_fixed,
               NVL (ve.bank_sales_channel, '0') bank_sales_channel,
               NVL (c.fronting_agent_comm_rate, 0) fronting_agent_comm_rate, --modular
               NVL (ve.insur_process_tax, 0) bsmv_tax,
               NVL (c.policy_type,0) direkt_policy_type,    --- direk endirek
               ve.endors_reason_code,
               NVL ((SELECT rate
                       FROM wip_koc_ocp_payment_plan
                      WHERE contract_id = p_contract_id
                            AND version_no is null
                            AND rownum = 1), 99) downpayment_rate,
               NVL((select aa.ceding_pct
                       from alz_rci_rate_details aa
                      where aa.rates_Set_id in (
                                                 select a.rates_set_id
                                                   from alz_rci_pol_rate_master  a
                                                  where a.contract_id  = p_contract_id
                                                    and a.partition_no = 1
                                                    and a.top_indicator = 'Y'
                                                )    and arrangement_id in(1 ,5)  --EXTF ve DOMF `dan herhangi biri veya her ikisi de girilmiþ ise
                        AND rownum = 1 ) , 0 )is_reinsured
        FROM   wip_policy_bases b,
               wip_koc_ocp_pol_versions_ext ve,
               wip_koc_ocp_pol_contracts_ext c,
               koc_dmt_agents_ext de,
               koc_mis_agent_group_ref agr,
               wip_policy_versions v,
               dmt_agents da,
               koc_ocp_pol_versions_ext ve2
       WHERE       b.contract_id = ve.contract_id
               AND b.contract_id = c.contract_id
               AND b.contract_id = p_contract_id
               AND b.agent_role = de.int_id
               AND de.mis_main_group = agr.mis_main_group
               AND de.mis_sub_group = agr.mis_sub_group
               AND b.contract_id = v.contract_id
               AND de.int_id = da.int_id
               AND ve.contract_id = ve2.contract_id(+)
               AND ve2.version_no(+) = 1;

   reco_policy               c_policy%ROWTYPE;

   CURSOR c_agent
   IS
      SELECT   c.agent_category_type category_type,
               b.reference_code code,
               b.int_id,
               c.mis_main_group main_group,
               d.explanation sales_channel,
               c.mis_sub_group sub_group,
               ve.region_code
        FROM   wip_policy_bases a,
               dmt_agents b,
               koc_dmt_agents_ext c,
               koc_mis_agent_group_ref d,
               wip_koc_ocp_pol_versions_ext ve
       WHERE       a.contract_id = p_contract_id
               AND a.contract_id = ve.contract_id
               AND a.agent_role = b.int_id
               AND b.int_id = c.int_id
               AND c.mis_main_group = d.mis_main_group
               AND c.mis_sub_group = d.mis_sub_group;

   reco_agent                c_agent%ROWTYPE;

   ---<--- TYH-66927 IDM Entegrasyonu -----
   -- user ,
   /*CURSOR c_user
   IS
      SELECT   v.username,
               par.first_name,
               par.surname,
               ext.TYPE user_type,
               get_user_level(v.username) user_level
        FROM   wip_policy_versions v,
               koc_cp_partners_ext cp,
               cp_partners par,
               koc_v_sec_system_users users,
               koc_dmt_agents_ext ext
       WHERE       v.contract_id = p_contract_id
               AND v.username = users.oracle_username
               AND cp.part_id = users.customer_partner_id
               AND cp.part_id = par.part_id
               AND ext.int_id = cp.agen_int_id;

   reco_user                 c_user%ROWTYPE;*/


   CURSOR c_wip_user
   IS
      SELECT v.username
        FROM wip_policy_versions v
       WHERE v.contract_id = p_contract_id;


   vv_username         VARCHAR2(500);
   vv_first_name     VARCHAR2(500);
   vv_surname         VARCHAR2(500);
   vv_user_type     VARCHAR2(10);
   vv_user_level     NUMBER;
   --->------------------------------------

   -- role ,
   CURSOR c_role (
      p_user VARCHAR2
   )
   IS
      SELECT   role_code
        FROM   koc_auth_user_role_rel a, wip_koc_ocp_pol_versions_ext v
       WHERE       username = p_user
               AND contract_id = p_contract_id
               AND a.validity_start_date <= v.signature_date
               AND (a.validity_end_date IS NULL
                    OR a.validity_end_date >= v.signature_date)
                    ORDER BY A.ROLE_CODE ASC;

   reco_role                 c_role%ROWTYPE;

   CURSOR c_partners_ph
   IS
      SELECT   DECODE (
                  (select count(*) from koc_cp_blacklist_entries where part_id in (
                    select part_id from koc_cp_partners_ext
                        where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                            AND from_date <= ve.signature_date
                            AND (TO_DATE IS NULL
                                 OR TO_DATE >= ve.signature_date)),
                  0,
                  '0',
                  '1'
                  )
                  blacklist,                                   --default value
                DECODE (
                    is_sisbis(p_contract_id, pe.identity_no, pe.tax_number, il.role_type),
                    0,
                    '0',
                    '1'
                 )
                 sisbis,
               cp.date_of_birth,
               DECODE (cp.partner_type, 'P', cp.first_name, cp.name)
                  first_name,
               pe.identity_no,
               cp.marital_status,
               cp.nationality,
               cp.part_id,
               cp.partner_type,
               --
               NVL (
                  DECODE (
                     (SELECT   1
                        FROM   koc_risk_partners
                       WHERE   (tax_number = pe.tax_number
                                OR identity_no = pe.identity_no)
                               AND entry_date <= ve.signature_date
                               AND (cancel_date IS NULL
                                    OR cancel_date >= ve.signature_date)
                               AND ROWNUM < 2),
                     1,
                     '1',
                     '0'
                  ),
                  '0'
               )
                  risky,                                       --default value
               --
               il.role_type,
               cp.surname,
               pe.tax_number,
               ip.action_code,
               DECODE (NVL (polcon.koc_family_ref_no, '0'), '0', 0, 1)
                  kocailem,
               cr.end_date,
               (CASE WHEN cr.part_id > 0 THEN 1 ELSE 0 END) hasrecord,
               get_eh_score(p_contract_id, pe.tax_number) eh_score,
               is_previously_denied(p_contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
               il.link_type,
               NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
               NVL(pe.is_foreign_company, 0) is_foreign_company
        FROM   wip_interested_parties ip,
               wip_ip_links il,
               wip_koc_ocp_pol_versions_ext ve,
               cp_partners cp,
               koc_cp_partners_ext pe,
               cp_relationships cr,
               koc_cp_relationships_ext cre,
               wip_koc_ocp_pol_contracts_ext polcon
       WHERE       ip.contract_id = p_contract_id
               AND ip.contract_id = il.contract_id
               AND ip.ip_no = il.ip_no
               AND ip.action_code <> 'D'
               AND ip.partner_id = cp.part_id
               AND cp.part_id = pe.part_id
               AND ip.contract_id = ve.contract_id
               AND il.role_type = 'PH'
               AND cre.part_id(+) = pe.part_id
               AND cr.part_id(+) = cre.part_id
               AND cr.relation_id(+) = cre.relation_id
               AND cr.rel_type(+) = cre.rel_type
               AND cr.from_date(+) = cre.from_date
               AND ip.contract_id = polcon.contract_id;

   reco_partners             c_partners_ph%ROWTYPE;

   CURSOR c_partitions
   IS
      SELECT    -- adress   coefficients   fire mkec extras   fire risk detail
            NVL (cp.net_premium, 0) + NVL (cp.tax_amount, 0)
                  gross_premium_by_partition_tl,
               -- insured partners    lcr partners
               NVL (cp.net_premium, 0) net_premium_by_partition_tl,
               -- notes
               p.partition_no,
               p.partition_type,
               -- policy partition covers
               NVL (
                  koc_sum_insured.get_sum_ins_wip (p.contract_id,
                                                   v.version_no,
                                                   p.partition_no,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   1,
                                                   v.business_start_date,
                                                   'TL')
                  * pe.def_sum_insured_swf_exchg
                  / koc_curr_utils.retrieve_effective_selling (
                       pe.def_sum_insured_swf,
                       v.business_start_date
                    ),
                  0
               )
                  sumins_whole_cover_by_risk_tl,              -- default value
               pe.def_sum_insured_swf swift_code,
               NVL (pe.def_sum_insured_swf_exchg, 1)
                  def_sum_insured_swf_exchg,                  -- default value
               p.action_code,
               NVL(pe.has_program_pol , 0) allianz_program_pol ,
               NVL(pe.has_fronting_pol ,0) allianz_fronting_pol,
               NVL(mn.job_type, 0) job_type
        FROM   wip_partitions p,
        WIP_KOC_OCP_CAR_EAR mn,
               wip_koc_ocp_partitions_ext pe,
               (  SELECT   partition_no,
                           SUM(NVL (final_premium, 0)
                               * NVL (premium_swf_exchg, 1))
                              net_premium,
                           SUM (
                              NVL (final_tax, 0) * NVL (premium_swf_exchg, 0)
                           )
                              tax_amount
                    FROM   wip_koc_ocp_policy_covers_ext
                   -- wip_policy_covers
                   WHERE   contract_id = p_contract_id
                GROUP BY   partition_no) cp,
               wip_policy_versions v
       WHERE       p.contract_id = pe.contract_id
               AND p.partition_no = pe.partition_no
               AND p.contract_id = p_contract_id
               AND p.partition_no = cp.partition_no
               AND p.contract_id = mn.contract_id
               AND p.contract_id = v.contract_id;

   reco_partitions           c_partitions%ROWTYPE;


   CURSOR c_clauses (
      p_partition_no IN NUMBER
   )
   IS
      SELECT   a.action_code,
               NVL (a.version_no, 0) version_no,
               NVL (a.object_id, 0) object_id,
               a.top_indicator,
               NVL (a.previous_version, 0) previous_version,
               NVL (a.reversing_version, 0) reversing_version,
               NVL (a.contract_id, 0) contract_id,
               NVL (a.partition_no, 0) partition_no,
               NVL (a.clause_id, 0) clause_id,
               a.ora_nls_code,
               NVL (a.order_no, 0) order_no,
               a.caluse_value_text,
               NVL (a.produced, 0) produced,
               NVL (a.is_mandatory, 0) is_mandatory,
               NVL (a.priority, 0) priority,
               a.prnt_place,
               NVL (a.prnt_on_quo_or_contract, 0) prnt_on_quo_or_contract
        FROM   wip_koc_ocp_partitions_clauses a
       WHERE       a.contract_id = p_contract_id
               AND a.partition_no = p_partition_no
               AND a.action_code <> 'D';

   reco_clauses              c_clauses%ROWTYPE;


   CURSOR adress_coordinates (
      p_partition_no IN NUMBER
   )
   IS
      SELECT   a.xcoor, a.ycoor
        FROM   wip_koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               wip_koc_ocp_pol_versions_ext e
       WHERE       b.contract_id = p_contract_id
               AND b.partition_no = p_partition_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND b.contract_id = e.contract_id;

   coordinates               ADRESS_COORDINATES%ROWTYPE;
   v_coordinate_found        NUMBER;
   v_signature_date          DATE;

   CURSOR earthquake_dates (
      p_xcoor   IN           NUMBER,
      p_ycoor   IN           NUMBER
   )
   IS
      SELECT   r.validity_start_date, r.validity_end_date
        FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
       WHERE       a.shape_id = r.shape_id
               AND r.risk_type = 2 --is kabul haddi
               AND r.dune_type = 2 --deprem riski
               AND r.auth_type = 2 --otorizasyona duser
               AND a.validity_start_date <= sysdate
               AND a.validity_end_date >= sysdate
               AND r.validity_start_date <= sysdate
               AND r.validity_end_date >= sysdate
               AND SDO_RELATE(
                     a.coordinates,
                     (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                     'mask=anyinteract'
                   )='TRUE';


   earthquakedates           earthquake_dates%ROWTYPE;
   v_default_end_date        DATE;

   CURSOR flood_dates (
      p_xcoor   IN            NUMBER,
      p_ycoor   IN            NUMBER
   )
   IS
      SELECT   r.validity_start_date, r.validity_end_date
        FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
       WHERE       a.shape_id = r.shape_id
               AND r.risk_type = 2 --is kabul haddi
               AND r.dune_type = 4 --sel riski
               AND r.auth_type = 2 --otorizasyona duser
               AND a.validity_start_date <= sysdate
               AND a.validity_end_date >= sysdate
               AND r.validity_start_date <= sysdate
               AND r.validity_end_date >= sysdate
               AND SDO_RELATE(
                     a.coordinates,
                     (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                     'mask=anyinteract'
                   )='TRUE';


   flooddates                flood_dates%ROWTYPE;

   CURSOR landslide_dates (
      p_xcoor   IN            NUMBER,
      p_ycoor   IN            NUMBER
   )
   IS
      SELECT   r.validity_start_date, r.validity_end_date
        FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
       WHERE       a.shape_id = r.shape_id
               AND r.risk_type = 2 --is kabul haddi
               AND r.dune_type = 6 --yer kaymasi riski
               AND r.auth_type = 2 --otorizasyona duser
               AND a.validity_start_date <= sysdate
               AND a.validity_end_date >= sysdate
               AND r.validity_start_date <= sysdate
               AND r.validity_end_date >= sysdate
               AND SDO_RELATE(
                     a.coordinates,
                     (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                     'mask=anyinteract'
                   )='TRUE';


   landslidedates            landslide_dates%ROWTYPE;

   CURSOR terror_dates (
      p_xcoor   IN            NUMBER,
      p_ycoor   IN            NUMBER
   )
   IS
      SELECT   r.validity_start_date, r.validity_end_date
        FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
       WHERE       a.shape_id = r.shape_id
               AND r.risk_type = 2 --is kabul haddi
               AND r.dune_type = 3 --teror riski
               AND r.auth_type = 2 --otorizasyona duser
               AND a.validity_start_date <= sysdate
               AND a.validity_end_date >= sysdate
               AND r.validity_start_date <= sysdate
               AND r.validity_end_date >= sysdate
               AND SDO_RELATE(
                     a.coordinates,
                     (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                     'mask=anyinteract'
                   )='TRUE';


   terrordates               terror_dates%ROWTYPE;

   CURSOR c_in_terror_list
   IS
     SELECT   1
       FROM   wip_koc_ocp_car_ear b,
              koc_cp_address_ext a,
              wip_koc_ocp_pol_versions_ext e,
              alz_terror_control t,
              wip_partitions p,
              wip_policy_contracts pc
      WHERE    b.contract_id = p_contract_id
              AND b.contract_id = p.contract_id
              AND b.partition_no = v_partition_no
              AND b.partition_no = p.partition_no
              AND b.contract_id = pc.contract_id
              AND b.risk_add_id = a.add_id
              AND b.contract_id = e.contract_id
              AND t.partition_type = p.partition_type
              AND t.product_id = pc.product_id
              AND t.city_code = a.city_code
              AND (t.district_code = a.district_code
                  OR NVL(lower(t.district_code), 'x') = 'x')         -- Ilin butun ilcelerinin otorizasyona dusmesi istendiginde
              --AND t.priority = 1
              --AND t.is_industrial = 1
              AND t.validity_start_date < trunc(sysdate)
              AND (t.validity_end_date > trunc(sysdate)
                  OR t.validity_end_date IS NULL);

   v_in_terror_list NUMBER(1);

   CURSOR c_address (
      p_earthquake_start_date   IN            DATE,
      p_earthquake_end_date     IN            DATE,
      p_flood_start_date        IN            DATE,
      p_flood_end_date          IN            DATE,
      p_landslide_start_date    IN            DATE,
      p_landslide_end_date         OUT        DATE,
      p_terror_start_date       IN            DATE,
      p_terror_end_date         IN            DATE
   )
   IS
      SELECT   b.risk_add_id,
               a.city_code,
               c.country_code,
               b.cresta_zone_code,
               a.district_code,
               p_earthquake_end_date earthquake_auth_end_date,
               p_earthquake_start_date earthquake_auth_start_date,
               b.earthquake_zone_code,
               p_flood_end_date flood_auth_end_date,
               p_flood_start_date flood_auth_start_date,
               p_landslide_end_date landslide_end_date,
               p_landslide_start_date landslide_start_date,
               b.municipality_code,
               a.quarter_code,
               p_terror_end_date terror_end_date,
               p_terror_start_date terror_start_date,
               koc_address_utils.address (b.risk_add_id) adres_txt
        FROM   wip_koc_ocp_car_ear b,
               koc_cp_address_ext a,
               cp_addresses c,
               wip_koc_ocp_pol_versions_ext e
       WHERE       b.contract_id = p_contract_id
               AND b.partition_no = v_partition_no
               AND b.risk_add_id = c.add_id
               AND c.add_id = a.add_id
               AND b.contract_id = e.contract_id;

   reco_address              c_address%ROWTYPE;

   CURSOR find_signature_date
   IS
      SELECT   e.signature_date
        FROM   wip_koc_ocp_pol_versions_ext e
       WHERE   e.contract_id = p_contract_id;

   CURSOR c_cumul (
      p_xcoor   IN            NUMBER,
      p_ycoor   IN            NUMBER
   )
   IS                                                                       --
      SELECT   /*+ index ( r koc_cumul_risk_def_idx2 ) */
            1
        FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
       WHERE       a.shape_id = r.shape_id
               AND r.area_type NOT IN ('10', '9')
               AND r.risk_type = 1 --kumul
               AND r.dune_type = 1 --yangin riski
               AND r.auth_type = 2 --otorizasyona duser
               AND a.validity_start_date <= sysdate
               AND a.validity_end_date >= sysdate
               AND r.validity_start_date <= sysdate
               AND r.validity_end_date >= sysdate
               AND SDO_RELATE(
                     a.coordinates,
                     (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                     'mask=anyinteract'
                   )='TRUE';

   reco_cumul                c_cumul%ROWTYPE;

   CURSOR c_coefficient
   IS                                                                       --
      SELECT   dr.action_code,
               dr.TYPE cotype,
               dr.discount_saving_code,
               dr.exe_amnt_or_rate,                          -- 1 oran 2 tutar
               dr.exemp_amount_swf,
               dr.exemption_type,
               -- 1 sigorta bedeli 2 hasar bedeli 3 ödenen tazminat 4 makina bedeli
               dr.dim_value exemption_value,
               NVL (dr.min_exemp_sum_ins, 0) min_exemp_sum_ins, --default value
               dr.min_exemp_sum_ins_swf,
               NVL (rate, 0) rate,                            -- default value
               NVL (dr.is_rate_enter_manually, 0) is_rate_enter_manually, --default value
               NVL (dr.tariff_disc_rate, 0) tariff_disc_rate,
               NVL (dr.document_group_id, 0) document_group_id -- default value
        FROM   wip_koc_ocp_disc_save_rates dr
       WHERE   contract_id = p_contract_id
               AND dr.partition_no = v_partition_no;

   reco_coefficient          c_coefficient%ROWTYPE;




   CURSOR c_activity_subject
   IS
      SELECT   segment, segment_code, subsegment, main_activity, sme_authorization, act_subj_exp
        FROM   koc_oc_act_subject_ref r,
               wip_koc_ocp_buildings bld,
               wip_policy_contracts c,
               wip_koc_ocp_liabilities l
       WHERE       bld.contract_id = p_contract_id
               AND bld.partition_no = v_partition_no
               AND bld.contract_id = l.contract_id(+)
               AND bld.partition_no = l.partition_no(+)
               AND c.contract_id = bld.contract_id
               AND bld.activity_subject_code = r.activity_subject_code
               AND (r.validity_start_date <= TRUNC (sysdate)
               AND ( (c.product_id IN (29, 30)
                      AND ( (r.validity_end_date IS NULL
                             AND r.sme_end_date IS NULL)
                           OR ( (r.validity_end_date IS NULL
                                 OR r.validity_end_date >= TRUNC (sysdate))
                               AND r.sme_end_date >= TRUNC (sysdate))))
                    OR (c.product_id NOT IN (29, 30)
                        AND (r.validity_end_date >= TRUNC (sysdate)
                             OR r.validity_end_date IS NULL)))
               )
   ;

   reco_activity_subject    c_activity_subject%ROWTYPE;

   CURSOR c_degree_of_risk
   IS
      SELECT   degree_of_risk
        FROM   wip_koc_ocp_buildings bld,
               wip_policy_contracts c,
               koc_oc_act_subject_ref_ext asr,
               wip_partitions p
       WHERE       bld.contract_id = p_contract_id
               AND bld.partition_no = v_partition_no
               AND c.contract_id = bld.contract_id
               AND bld.activity_subject_code = asr.activity_subject_code
               AND p.contract_id = bld.contract_id
               AND p.partition_type = asr.partition_type
               AND p.partition_no = bld.partition_no
               AND c.product_id = asr.product_id
               AND asr.validity_start_date <= TRUNC (sysdate)
               AND (asr.validity_end_date > TRUNC (sysdate)
               OR asr.validity_end_date IS NULL);

   v_degree_of_risk VARCHAR2(25);



   CURSOR c_note
   IS
      SELECT   NVL (a.has_new_notes, 0) has_new_notes,        -- default value
               NVL (a.u_w_edit, 0) u_w_edit,                    --default value
               a.note_type
        FROM   koc_notes_authorization a
       WHERE       ins_obj_uid = TO_CHAR (p_contract_id)
               AND a.sub_level2 = v_partition_no
               AND a.sub_level1 = (SELECT   NVL (version_no, 0) + 1
                                     FROM   wip_policy_versions
                                    WHERE   contract_id = p_contract_id);

   reco_note                 c_note%ROWTYPE;

   CURSOR c_cover
   IS
      SELECT   ce.action_code,                             --ocp_policy_covers
               NVL (ce.adjustment_sum_insured, 0)
               * NVL (ce.sum_insured_swf_exchg, 1)
                  adjustment_sum_insured_tl, -- default value
               NVL (c.sum_insured_whole_cover, 0)
               * NVL (ce.sum_insured_swf_exchg, 1)
                  cover_amount_tl, -- default value
               c.cover_code,
               ccd.cover_cat_group,
               cd.explanation,
               (NVL (ce.final_premium, 0) + NVL (ce.final_tax, 0))
               * NVL (ce.premium_swf_exchg, 1)
                  gross_premium_tl,
               NVL (ce.final_premium, 0) * NVL (ce.premium_swf_exchg, 1)
                  net_premium_tl,
               c.sum_insured_whole_cover_swf bedel_dvz_cinsi,
               ce.sum_insured_swf_exchg bedel_kur,
               c.ftpremium_or_whole_cover_swf prim_dvz_cinsi,
               ce.premium_swf_exchg prim_kur,
               NVL (gc.is_eff_total_sum_insured, 0) sigorta_bedeline_dahil,
               NVL (ce.full_cover, 0) tam_teminat,
               NVL (ce.comm_rate, 0) comm_rate,
               NVL (ce.tariff_comm_rate, 0) tariff_comm_rate,
               NVL (ce.clm_limit_per_year, 0)
               * NVL (ce.sum_insured_swf_exchg, 1) clm_limit_per_year
        FROM   wip_policy_covers c,
               wip_koc_ocp_policy_covers_ext ce,
               koc_v_cover cd,
               koc_oc_cover_definitions ccd,
               wip_koc_ocp_pol_versions_ext ve,
               wip_partitions p,
               wip_policy_contracts pc,
               koc_oc_general_cover_price gc
       WHERE       c.contract_id = p_contract_id
               AND c.contract_id = ce.contract_id
               AND c.partition_no = ce.partition_no
               AND c.cover_no = ce.cover_no
               AND c.cover_code = ce.cover_code
               AND c.contract_id = ve.contract_id
               AND c.cover_code = cd.cover_code
               AND c.cover_code = ccd.cover_code
               AND ccd.validity_start_date <= ve.signature_date
               AND c.partition_no = v_partition_no
               AND (ccd.validity_end_date IS NULL
                    OR ccd.validity_end_date > ve.signature_date)
               AND c.contract_id = p.contract_id
               AND c.partition_no = p.partition_no
               AND c.contract_id = pc.contract_id
               AND pc.product_id = gc.product_id
               AND p.partition_type = gc.partition_type
               AND c.cover_code = gc.cover_code;

   reco_cover                c_cover%ROWTYPE;

   CURSOR c_partners_partition (
      v_role_type VARCHAR2
   )
   IS                                                                       --
      SELECT   DECODE (
                  (select count(*) from koc_cp_blacklist_entries where part_id in (
                    select part_id from koc_cp_partners_ext
                        where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                            AND from_date <= ve.signature_date
                            AND (TO_DATE IS NULL
                                 OR TO_DATE >= ve.signature_date)),
                  0,
                  '0',
                  '1'
               )
                  blacklist,
                DECODE (
                    is_sisbis(p_contract_id, pe.identity_no, pe.tax_number, il.role_type),
                    0,
                    '0',
                    '1'
                 )
                 sisbis,
               cp.date_of_birth,
               DECODE (cp.partner_type, 'P', cp.first_name, cp.name)
                  first_name,
               pe.identity_no,
               cp.marital_status,
               cp.nationality,
               cp.part_id,
               cp.partner_type,
               --
               DECODE (
                  (SELECT   1
                     FROM   koc_risk_partners
                    WHERE   (tax_number = pe.tax_number
                             OR identity_no = pe.identity_no)
                            AND entry_date <= ve.signature_date
                            AND ROWNUM < 2
                            AND (cancel_date IS NULL
                                 OR cancel_date >= ve.signature_date)
                            AND ROWNUM < 2),
                  1,
                  '1',
                  '0'
               ),
               --
               il.role_type,
               cp.surname,
               pe.tax_number,
               ip.action_code,
               0 kocailem,
               NULL,
               0 hasrecord,
               get_eh_score(p_contract_id, pe.tax_number) eh_score,
               is_previously_denied(p_contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
               il.link_type,
               NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
               NVL(pe.is_foreign_company, 0)  is_foreign_company
        FROM   wip_interested_parties ip,
               wip_ip_links il,
               wip_koc_ocp_pol_versions_ext ve,
               cp_partners cp,
               koc_cp_partners_ext pe
       WHERE       ip.contract_id = p_contract_id
               AND ip.contract_id = il.contract_id
               AND il.partition_no = v_partition_no
               AND ip.ip_no = il.ip_no
               AND ip.action_code <> 'D'
               AND ip.partner_id = cp.part_id
               AND cp.part_id = pe.part_id
               AND ip.contract_id = ve.contract_id
               AND il.role_type = v_role_type;                         --'ph';

   wip_notfound_hata exception;

   --  Yeni eklemeler
   ----------------------------------------------------------------------------

   CURSOR c_fiziksel_bolge IS             -- Fiziksel Bölge Bilgisi eklendi.
    SELECT 1
      FROM koc_dmt_region_code_ref r, koc_dmt_v_current_agents d, wip_policy_bases o
     WHERE     o.contract_id = p_contract_id
           AND d.AGEN_INT_ID = o.AGENT_ROLE
           AND r.REGION_CODE= d.region_code
           AND r.PHY_REGION_CODE <> '55';

    v_fiziksel_bolge NUMBER(1);


   CURSOR c_branch
   IS
      SELECT b.branch_ext_ref
        FROM wip_koc_ocp_pol_versions_ext a,wip_koc_ocp_pol_contracts_ext b
       WHERE    a.CONTRACT_ID = p_contract_id
            AND b.CONTRACT_ID = a.CONTRACT_ID;

    v_branch_xref varchar2(10);


-------------------------------------------------------------------------------



   CURSOR c_parent_group_sub (
      p_group_code                 VARCHAR2,
      p_date                       DATE
   )
   IS
          SELECT   b.parent_group
            FROM   koc_cp_group_hierarchy b
           WHERE   LEVEL =
                      (    SELECT   MAX (LEVEL) - 1
                             FROM   koc_cp_group_hierarchy b
                       START WITH   b.child_group = p_group_code
                                    AND p_date >= b.validity_start_date
                                    AND (p_date < b.validity_end_date
                                         OR b.validity_end_date IS NULL)
                       CONNECT BY   PRIOR b.parent_group = b.child_group)
                   AND b.parent_group != '9999'
                   AND p_date >= b.validity_start_date
                   AND (p_date <= b.validity_end_date
                        OR b.validity_end_date IS NULL)
      START WITH   b.child_group = p_group_code
      CONNECT BY   PRIOR b.parent_group = b.child_group;

   reco_parent_group_sub     c_parent_group_sub%ROWTYPE;


   CURSOR c_payment_type_count (
      p_parent_group_code                 VARCHAR2,
      p_group_code                        VARCHAR2,
      p_date                              DATE
   )
   IS
      SELECT   COUNT ( * ) group_payment_count,
               MAX (payment_type) payment_type
        FROM   koc_cp_group_payment_rel a
       WHERE       group_code = p_group_code
               AND parent_group_code = p_parent_group_code
               AND validity_start_date <= p_date
               AND (validity_end_date IS NULL OR validity_end_date >= p_date);

   reco_payment_type_count   c_payment_type_count%ROWTYPE;

   CURSOR c1
   IS
      SELECT   a.group_code
        FROM   wip_koc_ocp_pol_versions_ext a
       WHERE   a.contract_id = p_contract_id
               AND NVL (a.group_code, ' ') = 'A001';

   CURSOR c2
   IS
      SELECT   a.move_code
        FROM   wip_policy_versions a
       WHERE   a.contract_id = p_contract_id;

   v_move_code               VARCHAR2 (10);

   CURSOR c_emps
   IS
      SELECT   ips.partner_id
        FROM   cp_v_relationships cpr,
               wip_ip_links ipl,
               wip_interested_parties ips,
               koc_cp_relationships_ext cpx
       WHERE       cpr.relation_id = 1
               AND cpr.part_id = ips.partner_id
               AND cpr.rel_type = 'JOB'
               AND cpr.part_1_quality = 'EMPE'
               AND ips.ip_no = ipl.ip_no
               AND ips.contract_id = ipl.contract_id
               AND ipl.contract_id = p_contract_id
               AND ipl.role_type = 'PH'
               AND ipl.action_code != 'D'
               AND ips.action_code != 'D'
               AND cpr.end_date IS NULL
               AND cpx.part_id = cpr.part_id
               AND cpx.rel_type = cpr.rel_type
               AND cpx.relation_id = 1
               AND cpx.employment_no IS NOT NULL;

   v_part_id                 NUMBER;

   v_emp_disc                NUMBER;
   c1_row                    c1%ROWTYPE;

   v_is_claimed number;
   v_is_payment_rate_changed number;

BEGIN
   OPEN c_policy;

   FETCH c_policy INTO reco_policy;

   IF c_policy%NOTFOUND
   THEN
      CLOSE c_policy;

      RAISE wip_notfound_hata;
   END IF;

   CLOSE c_policy;

   v_old_contract_id := bre_find_old_contract_id (p_contract_id);


   BEGIN
      SELECT   version_no
        INTO   v_version_no
        FROM   ocp_policy_versions
       WHERE   contract_id = v_old_contract_id AND top_indicator = 'Y';

      pme_api.set_query_version (v_version_no);
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END;

   OPEN c_parent_group_sub (reco_policy.group_code,
                            reco_policy.pol_signature_date);

   FETCH c_parent_group_sub INTO reco_parent_group_sub;

   IF NVL (reco_parent_group_sub.parent_group, '0') = '2222'
   THEN
      OPEN c_payment_type_count (reco_parent_group_sub.parent_group,
                                 reco_policy.group_code,
                                 reco_policy.pol_signature_date);

      FETCH c_payment_type_count
      INTO   reco_payment_type_count.group_payment_count,
             reco_payment_type_count.payment_type;

      CLOSE c_payment_type_count;

      IF     NVL (reco_payment_type_count.group_payment_count, 0) >= 1
         AND NVL (reco_policy.group_code, 'X') <> 'A008'
         AND NVL (reco_policy.aff_payment_type, 'X') = '1'
      THEN
         OPEN c1;

         FETCH c1 INTO c1_row;

         IF c1%NOTFOUND
         THEN
            v_emp_disc := NULL;
         ELSE
            OPEN c2;

            FETCH c2 INTO v_move_code;

            CLOSE c2;

            IF v_move_code IN ('ENBQ')
            THEN
               v_emp_disc := 1;
            ELSIF koc_motor_utils.get_session_form_mode = 4
            THEN                                                     -- teklif
               v_emp_disc := 1;
            ELSE
               OPEN c_emps;

               LOOP
                  FETCH c_emps INTO v_part_id;

                  EXIT WHEN c_emps%NOTFOUND;
                  v_emp_disc := 1;
               END LOOP;

               CLOSE c_emps;
            END IF;
         END IF;

         CLOSE c1;
      ELSE
         v_emp_disc := 0;
      END IF;
   ELSE
      v_emp_disc := 0;
   END IF;

   OPEN c_agent;
   FETCH c_agent INTO reco_agent;
   CLOSE c_agent;



   --banka YKB
   if reco_agent.code='10503' Then
      reco_policy.contract_type :=0;
   end if;

   OPEN c_fiziksel_bolge;
   FETCH c_fiziksel_bolge INTO v_fiziksel_bolge;

    IF c_fiziksel_bolge%NOTFOUND
    THEN
      v_fiziksel_bolge :=0;
    END IF;
    CLOSE c_fiziksel_bolge;

   OPEN c_branch;
   FETCH c_branch INTO v_branch_xref;
   CLOSE c_branch;

   v_is_payment_rate_changed := is_payment_rate_changed(p_contract_id, reco_policy.product_id );

   p_wip_policy :=
      CAREAR_BRE_policy (reco_policy.contract_id,
                      reco_policy.version_no,
                      reco_policy.policy_type,
                      reco_policy.term_start_date,
                      reco_policy.signature_date,
                      reco_policy.term_end_date,
                      reco_policy.product_id,
                      reco_policy.business_start_date,
                      reco_policy.group_code,
                      NULL,                                   -- CAREAR_BRE_agent
                      NULL,                                    -- CAREAR_BRE_user
                      reco_policy.endorsement_no,
                      reco_policy.contract_type,
                      reco_policy.reference_contract_id,
                      reco_policy.oip_policy_ref,
                      reco_policy.gross_premium_tl,
                      reco_policy.usd_exchg_rate,
                      reco_policy.eur_exchg_rate,
                      reco_policy.net_premium_tl,
                      reco_policy.is_renewal,
                      reco_policy.old_contract_id,
                      reco_policy.pol_signature_date,
                      reco_policy.is_industrial,
                      NULL,
                      NULL,
                      NULL,
                      reco_policy.is_date_fixed,
                      reco_policy.aff_payment_type,
                      reco_policy.bank_sales_channel,
                      reco_policy.fronting_agent_comm_rate,
                      v_branch_xref,
                      v_fiziksel_bolge,
                      reco_policy.endors_reason_code,
                      reco_policy.adj_gross_premium_tl,
                      reco_policy.bsmv_tax,
                      reco_policy.downpayment_rate,    --peþinat
                      0, --reco_policy.sum_imsured_total,   --
                      reco_policy.direkt_policy_type,       --direk endirek
                      reco_policy.is_reinsured,
                      v_is_payment_rate_changed
                     );


   IF nvl(reco_agent.int_id,0) = 0 THEN
      NULL;
   ELSE
      p_wip_policy.agent :=
         CAREAR_BRE_agent (reco_agent.int_id,
                        reco_agent.code,
                        reco_agent.sales_channel,
                        reco_agent.main_group,
                        reco_agent.sub_group,
                        reco_agent.category_type,
                        reco_agent.region_code);
   END IF;


   ---<--- TYH-66927 IDM Entegrasyonu -----
   -- user / role
   /*OPEN c_user;

   FETCH c_user INTO reco_user;

   IF c_user%NOTFOUND
   THEN
      NULL;
   ELSE
      p_wip_policy.carear_user :=
         CAREAR_BRE_user (reco_user.username,
                       reco_user.first_name,
                       reco_user.surname,
                       NULL,
                       reco_user.user_type,
                       reco_user.user_level);
      v_count := 0;
      p_wip_policy.carear_user.role_code := CAREAR_BRE_role_table ();

      OPEN c_role (reco_user.username);

      LOOP
         FETCH c_role INTO reco_role;

         EXIT WHEN c_role%NOTFOUND;
         v_count := v_count + 1;
         p_wip_policy.carear_user.role_code.EXTEND;
         p_wip_policy.carear_user.role_code (v_count) :=
            CAREAR_BRE_role (reco_role.role_code);
      END LOOP;

      CLOSE c_role;

      IF v_count = 0
      THEN
         p_wip_policy.carear_user.role_code := NULL;
      END IF;
   END IF;

   CLOSE c_user;*/

   vv_username := NULL;
   vv_first_name := NULL;
   vv_surname := NULL;
   vv_user_type := NULL;


   open c_wip_user;
   fetch c_wip_user into vv_username;
   close c_wip_user;

   alz_base_function_utils.user_type(vv_username, vv_first_name, vv_surname, vv_user_type);
   IF vv_first_name IS NOT NULL THEN
      vv_user_level := get_user_level(vv_username);
      p_wip_policy.carear_user :=
         CAREAR_BRE_user (vv_username,
                       vv_first_name,
                       vv_surname,
                       NULL,
                       vv_user_type,
                       vv_user_level);
      v_count := 0;
      p_wip_policy.carear_user.role_code := CAREAR_BRE_role_table ();

      OPEN c_role (vv_username);

      LOOP
         FETCH c_role INTO reco_role;

         EXIT WHEN c_role%NOTFOUND;
         v_count := v_count + 1;
         p_wip_policy.carear_user.role_code.EXTEND;
         p_wip_policy.carear_user.role_code (v_count) :=
            CAREAR_BRE_role (reco_role.role_code);
      END LOOP;

      CLOSE c_role;

      IF v_count = 0
      THEN
         p_wip_policy.carear_user.role_code := NULL;
      END IF;
   END IF;

   --->------------------------------------

   --
   v_count := 0;
   p_wip_policy.policy_holders := CAREAR_BRE_partner_table ();

   OPEN c_partners_ph;

   LOOP
      FETCH c_partners_ph INTO reco_partners;

      EXIT WHEN c_partners_ph%NOTFOUND;
      multiplefactorpolicy (reco_partners.part_id,
                      reco_agent.int_id,
                      'PH',
                      v_found_factor);
--      multiplepolicy (reco_partners.identity_no,
--                      reco_partners.tax_number,
--                      reco_agent.int_id,
--                      'PH',
--                      v_found);

      v_multi_pol_same_quarter_found := has_multi_pol_same_address(p_contract_id, 1 , reco_agent.int_id , 'PH');

      v_count := v_count + 1;
      p_wip_policy.policy_holders.EXTEND;
      p_wip_policy.policy_holders (v_count) :=
         CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          v_found_factor,
                          reco_partners.kocailem,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,   --v_found
                          v_multi_pol_same_quarter_found);
   END LOOP;

   CLOSE c_partners_ph;

   IF v_count = 0
   THEN
      p_wip_policy.policy_holders := NULL;
   END IF;

   OPEN find_signature_date;
   FETCH find_signature_date INTO v_signature_date;
   CLOSE find_signature_date;

   v_count := 0;
   p_wip_policy.policy_partitions := CAREAR_BRE_partition_table ();

   OPEN c_partitions;

   LOOP
      FETCH c_partitions INTO reco_partitions;

      EXIT WHEN c_partitions%NOTFOUND;
      v_partition_no := reco_partitions.partition_no;
      v_count := v_count + 1;

      v_quote_same_quarter_found := has_quote_same_quarter(p_contract_id);

      p_wip_policy.policy_partitions.EXTEND;
      p_wip_policy.policy_partitions (v_count) :=
         CAREAR_BRE_partition (reco_partitions.action_code,
                            reco_partitions.partition_no,
                            reco_partitions.partition_type,
                            reco_partitions.sumins_whole_cover_by_risk_tl,
                            reco_partitions.net_premium_by_partition_tl,
                            reco_partitions.gross_premium_by_partition_tl,
                            reco_partitions.swift_code,
                            reco_partitions.def_sum_insured_swf_exchg,
                            NULL,                                    -- covers
                            NULL,                                     -- notes
                            NULL,                              -- coefficients
                            NULL,                              -- lcr_partners
                            NULL,                           --insured_partners
                            NULL,                           -- address
                            NULL,                                   --clauses
                            NULL,                          -- activity_subject
                            v_quote_same_quarter_found,     -- quote_same_quarter
                            reco_partitions.allianz_program_pol ,
                            reco_partitions.allianz_fronting_pol ,
                            reco_partitions.job_type );

      OPEN adress_coordinates (v_partition_no);
      FETCH adress_coordinates INTO coordinates;
      IF adress_coordinates%NOTFOUND THEN
         coordinates := NULL;
      END IF;
      CLOSE adress_coordinates;

      --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri aliniyor
      IF coordinates.xcoor IS NOT NULL AND coordinates.ycoor IS NOT NULL
      THEN
         v_coordinate_found := 1;

         OPEN earthquake_dates (coordinates.xcoor, coordinates.ycoor);
         FETCH earthquake_dates INTO earthquakedates;
         IF earthquake_dates%NOTFOUND THEN
            earthquakedates := NULL;
         END IF;
         CLOSE earthquake_dates;

         OPEN flood_dates (coordinates.xcoor, coordinates.ycoor);
         FETCH flood_dates INTO flooddates;
         IF flood_dates%NOTFOUND THEN
            flooddates := NULL;
         END IF;
         CLOSE flood_dates;

         OPEN landslide_dates (coordinates.xcoor, coordinates.ycoor);
         FETCH landslide_dates INTO landslidedates;
         IF landslide_dates%NOTFOUND THEN
            landslidedates := NULL;
         END IF;
         CLOSE landslide_dates;

         OPEN terror_dates (coordinates.xcoor, coordinates.ycoor);
         FETCH terror_dates INTO terrordates;
         IF (terror_dates%NOTFOUND OR terrordates.validity_end_date < sysdate)THEN
              OPEN c_in_terror_list;
              FETCH c_in_terror_list INTO v_in_terror_list;
              IF c_in_terror_list%NOTFOUND THEN
                terrordates := NULL;
              ELSE -- Teror il/ilce listesindeyse start/end date'i otorizasyona dusecek sekilde set et
                terrordates.validity_start_date := sysdate-2;
                terrordates.validity_end_date := sysdate+2;
              END IF;
              CLOSE c_in_terror_list;
         END IF;
         CLOSE terror_dates;

      END IF;
      --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri alindi

      IF v_coordinate_found = 1
      THEN
         OPEN c_address (earthquakedates.validity_start_date,
                         earthquakedates.validity_end_date,
                         flooddates.validity_start_date,
                         flooddates.validity_end_date,
                         landslidedates.validity_start_date,
                         landslidedates.validity_end_date,
                         terrordates.validity_start_date,
                         terrordates.validity_end_date);
      ELSE
         OPEN c_address (v_signature_date - 1,
                         v_default_end_date,
                         v_signature_date - 1,
                         v_default_end_date,
                         v_signature_date - 1,
                         v_default_end_date,
                         v_signature_date - 1,
                         v_default_end_date);
      END IF;

      FETCH c_address INTO reco_address;

      IF c_address%NOTFOUND
      THEN
         NULL;
      ELSE
         p_wip_policy.policy_partitions (v_count).address :=
            CAREAR_BRE_address (
               CAREAR_BRE_basic_address (reco_address.risk_add_id,
                                      reco_address.country_code,
                                      reco_address.city_code,
                                      reco_address.district_code,
                                      reco_address.quarter_code,
                                      reco_address.adres_txt),
               reco_address.municipality_code,
               reco_address.earthquake_zone_code,
               reco_address.cresta_zone_code,
               reco_address.earthquake_auth_start_date,
               reco_address.earthquake_auth_end_date,
               reco_address.flood_auth_start_date,
               reco_address.flood_auth_end_date,
               -- landslide mahalle bazyna kadar inebilmekte  ilçede olmayyp mahallede olabilir eklenmesi gerekir.
               reco_address.landslide_start_date,
               reco_address.landslide_end_date,
               reco_address.terror_start_date,
               reco_address.terror_end_date,
               NULL                                                    --cumul
            );

         IF coordinates.xcoor IS NOT NULL AND coordinates.ycoor IS NOT NULL
         THEN
            OPEN c_cumul (coordinates.xcoor, coordinates.ycoor);

            FETCH c_cumul INTO reco_cumul;

            IF c_cumul%NOTFOUND
            THEN
               NULL;
            ELSE
               p_wip_policy.policy_partitions (v_count).address.cumul :=
                  CAREAR_BRE_cumul (1, 'H', NULL);
            END IF;

            CLOSE c_cumul;
         ELSE
            p_wip_policy.policy_partitions (v_count).address.cumul :=
               CAREAR_BRE_cumul (1, 'H', NULL);
         END IF;
      END IF;

      CLOSE c_address;

      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).coefficients :=
         CAREAR_BRE_coef_table ();

      OPEN c_coefficient;

      LOOP
         FETCH c_coefficient INTO reco_coefficient;

         EXIT WHEN c_coefficient%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).coefficients.EXTEND;
         p_wip_policy.policy_partitions (v_count).coefficients (v_count2) :=
            CAREAR_BRE_coefficient (reco_coefficient.discount_saving_code,
                                 -- coefficientcode
                                 reco_coefficient.cotype,
                                 reco_coefficient.action_code,
                                 reco_coefficient.rate,
                                 reco_coefficient.exemption_value,
                                 reco_coefficient.exemption_type,
                                 reco_coefficient.is_rate_enter_manually,
                                 reco_coefficient.min_exemp_sum_ins,
                                 reco_coefficient.min_exemp_sum_ins_swf,
                                 reco_coefficient.exemp_amount_swf,
                                 reco_coefficient.tariff_disc_rate,
                                 reco_coefficient.exe_amnt_or_rate,
                                 reco_coefficient.document_group_id);
      END LOOP;

      CLOSE c_coefficient;

      IF v_count2 = 0
      THEN
         p_wip_policy.policy_partitions (v_count).coefficients := NULL;
      END IF;


      OPEN c_activity_subject;

      FETCH c_activity_subject INTO reco_activity_subject;

      IF c_activity_subject%NOTFOUND
      THEN
         NULL;
      ELSE
          OPEN c_degree_of_risk;

          FETCH c_degree_of_risk INTO v_degree_of_risk;

          CLOSE c_degree_of_risk;

         p_wip_policy.policy_partitions (v_count).activity_subject :=
            CAREAR_BRE_activity_subject (reco_activity_subject.segment,
                                        reco_activity_subject.segment_code,
                                        reco_activity_subject.subsegment,
                                        reco_activity_subject.main_activity,
                                        NVL(v_degree_of_risk, ' '),
                                        reco_activity_subject.act_subj_exp
                                      );
      END IF;

      CLOSE c_activity_subject;




      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).notes := CAREAR_BRE_note_table ();

      OPEN c_note;

      LOOP
         FETCH c_note INTO reco_note;

         EXIT WHEN c_note%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).notes.EXTEND;
         p_wip_policy.policy_partitions (v_count).notes (v_count2) :=
            CAREAR_BRE_note (reco_note.has_new_notes,
                             reco_note.u_w_edit,
                             reco_note.note_type );
      END LOOP;

      CLOSE c_note;

      IF v_count2 = 0
      THEN
         p_wip_policy.policy_partitions (v_count).notes := NULL;
      END IF;


      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).clauses :=
         CAREAR_BRE_clause_table ();

      OPEN c_clauses (v_partition_no);

      LOOP
         FETCH c_clauses INTO reco_clauses;

         EXIT WHEN c_clauses%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).clauses.EXTEND;
         p_wip_policy.policy_partitions (v_count).clauses (v_count2) :=
            CAREAR_BRE_clause (reco_clauses.action_code,
                            reco_clauses.version_no,
                            reco_clauses.object_id,
                            reco_clauses.top_indicator,
                            reco_clauses.previous_version,
                            reco_clauses.reversing_version,
                            reco_clauses.contract_id,
                            reco_clauses.partition_no,
                            reco_clauses.clause_id,
                            0,                    --reco_clauses.ora_nls_code,
                            reco_clauses.order_no,
                            reco_clauses.caluse_value_text,
                            reco_clauses.produced,
                            reco_clauses.is_mandatory,
                            reco_clauses.priority,
                            reco_clauses.prnt_place,
                            reco_clauses.prnt_on_quo_or_contract);
      END LOOP;

      CLOSE c_clauses;

      IF v_count2 = 0
      THEN
         p_wip_policy.policy_partitions (v_count).clauses := NULL;
      END IF;



      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).covers :=
         CAREAR_BRE_cover_table ();

      OPEN c_cover;

      LOOP
         FETCH c_cover INTO reco_cover;

         EXIT WHEN c_cover%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).covers.EXTEND;
         p_wip_policy.policy_partitions (v_count).covers (v_count2) :=
            CAREAR_BRE_cover (reco_cover.cover_code,
                           reco_cover.explanation,
                           --cover_type              ,
                           reco_cover.cover_cat_group,
                           reco_cover.cover_amount_tl,
                           reco_cover.adjustment_sum_insured_tl,
                           reco_cover.net_premium_tl,
                           reco_cover.gross_premium_tl,
                           reco_cover.action_code,
                           reco_cover.bedel_dvz_cinsi,
                           reco_cover.bedel_kur,
                           reco_cover.prim_dvz_cinsi,
                           reco_cover.prim_kur,
                           reco_cover.sigorta_bedeline_dahil,
                           reco_cover.tam_teminat,
                           reco_cover.comm_rate,
                           reco_cover.tariff_comm_rate,
                           reco_cover.clm_limit_per_year
                           );
      END LOOP;

      CLOSE c_cover;

      IF v_count2 = 0
      THEN
         p_wip_policy.policy_partitions (v_count).covers := NULL;
      END IF;

      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).insured_partners :=
         CAREAR_BRE_partner_table ();

      OPEN c_partners_partition ('INS');

      LOOP
         FETCH c_partners_partition INTO reco_partners;

         EXIT WHEN c_partners_partition%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).insured_partners.EXTEND;
         multiplefactorpolicy (reco_partners.part_id,
                         reco_agent.int_id,
                         'INS',
                         v_found_factor);
--         multiplepolicy (reco_partners.identity_no,
--                         reco_partners.tax_number,
--                         reco_agent.int_id,
--                         'INS',
--                         v_found);
         v_multi_pol_same_quarter_found := has_multi_pol_same_address(p_contract_id, 1 , reco_agent.int_id , 'INS');

         p_wip_policy.policy_partitions (v_count).insured_partners (v_count2) :=
            CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          v_found_factor,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0, --v_found
                          v_multi_pol_same_quarter_found);
      END LOOP;

      CLOSE c_partners_partition;

      IF v_count = 0
      THEN
         p_wip_policy.policy_partitions (v_count).insured_partners := NULL;
      END IF;

      v_count2 := 0;
      p_wip_policy.policy_partitions (v_count).lcr_partners :=
         CAREAR_BRE_partner_table ();

      OPEN c_partners_partition ('LCR');

      LOOP
         FETCH c_partners_partition INTO reco_partners;

         EXIT WHEN c_partners_partition%NOTFOUND;
         v_count2 := v_count2 + 1;
         p_wip_policy.policy_partitions (v_count).lcr_partners.EXTEND;
         p_wip_policy.policy_partitions (v_count).lcr_partners (v_count2) :=
            CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
      END LOOP;

      CLOSE c_partners_partition;

      IF v_count = 0
      THEN
         p_wip_policy.policy_partitions (v_count).lcr_partners := NULL;
      END IF;
   END LOOP;

   CLOSE c_partitions;

   IF v_count = 0
   THEN
      p_wip_policy.policy_partitions := NULL;
   END IF;

   -- önceki poliçe ve hasarlar

   --v_yil_count:=0;
   v_count := 0;
   v_count2 := 0;
   p_wip_policy.prev_carear_claims := CAREAR_BRE_prev_claim_table ();
   v_contract_id := p_contract_id;




      IF v_contract_id <> 0
      THEN
         v_count := v_count + 1;
         p_claim_info (v_contract_id, v_claims);

         IF v_claims IS NOT NULL
         THEN
            v_count2 := v_count2 + 1;
            p_wip_policy.prev_carear_claims.EXTEND;
            p_wip_policy.prev_carear_claims (v_count2) :=
               CAREAR_BRE_prev_claim (NULL,
                                   NULL,
                                   NULL,
                                   NULL);
            p_wip_policy.prev_carear_claims (v_count2).contractid :=
               v_contract_id;
            p_wip_policy.prev_carear_claims (v_count2).yearorderno := v_count;

            SELECT   policy_ref
              INTO   p_wip_policy.prev_carear_claims (v_count2).claimpolicyno
              FROM   ocp_policy_bases
             WHERE   contract_id = v_contract_id AND ROWNUM < 2;

            p_wip_policy.prev_carear_claims (v_count2).carearclaimlist := v_claims;
         END IF;

      ELSE
         begin
           SELECT p.is_claimed
              into v_is_claimed
            FROM iris_cnv_policy_contract_rel r, iris_cnv_policy p
            WHERE r.contract_id = p_contract_id AND r.policy_no = p.policy_no;
         exception when others then
           v_is_claimed:=0;
         end;
         --hayri
         if nvl(v_is_claimed,0)=0 then
            begin
              SELECT p.is_claimed
                 into v_is_claimed
               FROM GWS_POLICY_CONTRACT r, GWS_POLICY p
               WHERE r.contract_id = p_contract_id AND r.POLICY_NO = p.SOURCE_POLICYNO;
            exception when others then
              v_is_claimed:=0;
            end;
         end if;

        IF v_is_claimed = 1 THEN
           v_count := v_count + 1;

             v_claims := CAREAR_BRE_claim_table ();
             v_claims.EXTEND;
             v_claims (v_count) :=
                CAREAR_BRE_claim ('YKS',
                               'YB',
                               null,
                               0,
                               300000,
                               300000,
                               NULL,            --partners CAREAR_BRE_PARTNER_TABLE ,
                               0,      --rucuRate number,
                               NULL          --basic_address CAREAR_BRE_basic_address
                                   );


           IF v_claims IS NOT NULL THEN
              v_count2 := v_count2 + 1;
              p_wip_policy.prev_carear_claims.EXTEND;
              p_wip_policy.prev_carear_claims (v_count2) :=CAREAR_BRE_prev_claim (NULL, NULL, NULL,NULL);

              p_wip_policy.prev_carear_claims (v_count2).yearorderno :=  v_count;
              p_wip_policy.prev_carear_claims (v_count2).contractid := 0;
              p_wip_policy.prev_carear_claims (v_count2).claimpolicyno := NULL;
              p_wip_policy.prev_carear_claims (v_count2).carearclaimlist :=  v_claims;

           END IF;
        END IF;
     END IF;



   IF v_count2 = 0
   THEN                         -- eski poliçesi olsa da , hasari yoksa , null
      p_wip_policy.prev_carear_claims := NULL;
   END IF;

   RETURN;
EXCEPTION
   WHEN wip_notfound_hata
   THEN
      alz_web_process_utils.process_result (0,
                                            9,
                                            -1,
                                            'INVALID_DATA',
                                            'WIP kayit bulunamadi',
                                            'WIP kayit bulunamadi',
                                            NULL,
                                            NULL,
                                            'alz_carear_bre_utils , p_wip',
                                            NULL,
                                            p_process_results);
END p_wip;

---------------



   -- OCP , PREV
   PROCEDURE p_ocp_prv (
      p_contract_id           NUMBER,
      wip_contract_id         NUMBER,            -- cursor c_note ve c_cover için gerekli
      p_ocp_prv_policy    OUT CAREAR_BRE_policy,
      p_process_results   OUT customer.process_result_table
   )
   IS
      v_count              INT;
      v_count2             INT;
      v_partition_no       NUMBER (5);
      v_contract_id        NUMBER (10);
      v_version_no         NUMBER (5);
      v_found_factor              NUMBER (1);
      v_found                     NUMBER (1);

      CURSOR c_policy
      IS
         SELECT   b.contract_id,
                  NVL (v.version_no, 0) version_no, -- default value
                  DECODE (bre_find_old_contract_id (p_contract_id), NULL, '0', '1')
                     policy_type,
                  b.term_start_date,
                  ve.signature_date,
                  b.term_end_date,
                  v.product_id,
                  v.business_start_date,
                  ve.group_code,
                  NVL (ve.endorsement_no, 0) endorsement_no,  -- DEFAULT VALUE
                  (CASE
                      WHEN c.contract_type = 0 AND c.prev_quote_ref IS NULL
                      THEN
                         2
                      WHEN c.contract_type = 0
                           AND c.prev_quote_ref IS NOT NULL
                      THEN
                         3
                      WHEN c.contract_type = 1
                           AND v.movement_reason_code <> 'ENBQ'
                      THEN
                         0
                      WHEN c.contract_type = 1
                           AND v.movement_reason_code = 'ENBQ'
                      THEN
                         1
                   END)
                     contract_type, --0=Poliçe, 1=Teklife Istinaden Poliçe ,2=Teklif, 3=Teklife Istinaden Teklif
                  NVL (c.reference_contract_id, 0) reference_contract_id, -- DEFAULT VALUE
                  c.oip_policy_ref,
                    NVL (v.full_term_premium, 0)
                  + NVL (ve.fire_tax, 0)
                  + NVL (ve.insur_process_tax, 0)
                     gross_premium_tl,
                     (  NVL (v.adjustment_premium, 0)
                   + NVL (ve.adj_insur_process_tax, 0)
                   + NVL (ve.adj_traffic_fund, 0)
                   + NVL (ve.adj_guarantee_fund, 0)
                   + NVL (ve.adj_fire_tax, 0))
                     adj_gross_premium_tl,
                  NVL (
                     koc_curr_utils.retrieve_currency_buying (
                        'USD',
                        v.business_start_date
                     ),
                     1
                  )
                     usd_exchg_rate,                          -- DEFAULT VALUE
                  NVL (
                     koc_curr_utils.retrieve_currency_buying (
                        'EUR',
                        v.business_start_date
                     ),
                     1
                  )
                     eur_exchg_rate,                          -- DEFAULT VALUE
                  NVL (v.full_term_premium, 0) net_premium_tl, --default value
                  NVL (c.is_renewal, 0) is_renewal,           -- default value
                  NVL (c.old_contract_id, 0) old_contract_id, -- default value
                  ve2.signature_date pol_signature_date,
                  NVL(ve2.is_industrial , 0) is_industrial,
                  NVL (ve.AFF_PAYMENT_TYPE, '0') AFF_PAYMENT_TYPE,
                  NVL (ve.IS_DATE_FIXED, 0) IS_DATE_FIXED,
                  NVL (ve.bank_sales_channel, '0') bank_sales_channel,
                  NVL (c.fronting_agent_comm_rate, 0) fronting_agent_comm_rate, --modular
                  NVL (ve.insur_process_tax, 0) bsmv_tax,
                  NVL (c.policy_type,0) direkt_policy_type,    --- direk endirek
                  ve.endors_reason_code,
                  NVL ((SELECT rate
                          FROM koc_ocp_payment_plan
                         WHERE contract_id = p_contract_id
                               AND version_no is null
                               AND rownum = 1), 0) downpayment_rate,
                  NVL((select aa.ceding_pct
                         from alz_rci_rate_details aa
                        where aa.rates_Set_id in (
                                                 select a.rates_set_id
                                                   from alz_rci_pol_rate_master  a
                                                  where a.contract_id  = p_contract_id
                                                    and a.partition_no = 1
                                                --    and a.reins_cover_group = 174     --Gelen deðer 100 deðilse otorizasyona düþer
                                                    and a.top_indicator = 'Y'
                                                )   and arrangement_id = 374
                    AND rownum = 1 ) , 0 )is_reinsured
           FROM   pme_bv_policy_bases b,
                  bv_koc_ocp_pol_versions_ext ve,
                  bv_koc_ocp_pol_contracts_ext c,
                  koc_dmt_agents_ext de,
                  koc_mis_agent_group_ref agr,
                  pme_bv_policy_versions v,
                  dmt_agents da,
                  koc_ocp_pol_versions_ext ve2
          WHERE       b.contract_id = ve.contract_id
                  AND b.contract_id = c.contract_id
                  AND b.contract_id = p_contract_id
                  AND b.agent_role = de.int_id
                  AND de.mis_main_group = agr.mis_main_group
                  AND de.mis_sub_group = agr.mis_sub_group
                  AND b.contract_id = v.contract_id
                  AND de.int_id = da.int_id
                  AND ve.contract_id = ve2.contract_id(+)
                  AND ve2.version_no(+) = 1;

      reco_policy          c_policy%ROWTYPE;

      CURSOR c_agent
      IS
         SELECT   c.agent_category_type category_type,
                  b.reference_code code,
                  b.int_id,
                  c.mis_main_group main_group,
                  d.explanation sales_channel,
                  c.mis_sub_group sub_group,
                  ve.region_code
           FROM   pme_bv_policy_bases a,
                  dmt_agents b,
                  koc_dmt_agents_ext c,
                  koc_mis_agent_group_ref d,
                  bv_koc_ocp_pol_versions_ext ve
          WHERE       a.contract_id = p_contract_id
                  AND a.contract_id = ve.contract_id
                  AND a.agent_role = b.int_id
                  AND b.int_id = c.int_id
                  AND c.mis_main_group = d.mis_main_group
                  AND c.mis_sub_group = d.mis_sub_group
                  AND ve.version_no = (SELECT MAX (version_no)
                                         FROM koc_ocp_pol_versions_ext
                                        WHERE contract_id = p_contract_id);

      reco_agent           c_agent%ROWTYPE;

      ---<--- TYH-66927 IDM Entegrasyonu -----
      -- user ,
      /*CURSOR c_user
      IS
         SELECT   v.username,
               par.first_name,
               par.surname,
               ext.TYPE user_type,
               get_user_level(v.username) user_level
        FROM   pme_bv_policy_versions v,
               koc_cp_partners_ext cp,
               cp_partners par,
               koc_v_sec_system_users users,
               koc_dmt_agents_ext ext
          WHERE       v.contract_id = p_contract_id
               AND v.username = users.oracle_username
               AND cp.part_id = users.customer_partner_id
               AND cp.part_id = par.part_id
               AND ext.int_id = cp.agen_int_id;

      reco_user                 c_user%ROWTYPE;*/


      CURSOR c_pme_user
      IS
         SELECT v.username
        FROM pme_bv_policy_versions v
          WHERE v.contract_id = p_contract_id;


      vv_username         VARCHAR2(500);
      vv_first_name     VARCHAR2(500);
      vv_surname         VARCHAR2(500);
      vv_user_type     VARCHAR2(10);
      vv_user_level     NUMBER;
      --->------------------------------------

      -- role ,
      CURSOR c_role (
         p_user VARCHAR2
      )
      IS
         SELECT   role_code
           FROM   koc_auth_user_role_rel a, bv_koc_ocp_pol_versions_ext v
          WHERE       username = p_user
                  AND contract_id = p_contract_id
                  AND a.validity_start_date <= v.signature_date
                  AND (a.validity_end_date IS NULL
                       OR a.validity_end_date >= v.signature_date)
                       ORDER BY A.ROLE_CODE ASC;

      reco_role            c_role%ROWTYPE;

      CURSOR c_partners_ph
      IS
         SELECT   DECODE (
                      (select count(*) from koc_cp_blacklist_entries where part_id in (
                        select part_id from koc_cp_partners_ext
                            where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                                AND from_date <= ve.signature_date
                                AND (TO_DATE IS NULL
                                     OR TO_DATE >= ve.signature_date)),
                      0,
                      '0',
                      '1'
                    )
                     blacklist,
                  DECODE (
                      is_sisbis(p_contract_id, pe.identity_no, pe.tax_number, il.role_type),
                      0,
                      '0',
                      '1'
                  )
                  sisbis,
                  cp.date_of_birth,
                  DECODE (cp.partner_type, 'P', cp.first_name, cp.NAME)
                     first_name,
                  pe.identity_no,
                  cp.marital_status,
                  cp.nationality,
                  cp.part_id,
                  cp.partner_type,
                  --
                  NVL (
                     DECODE (
                        (SELECT   1
                           FROM   koc_risk_partners
                          WHERE   (tax_number = pe.tax_number
                                   OR identity_no = pe.identity_no)
                                  AND entry_date <= ve.signature_date
                                  AND (cancel_date IS NULL
                                       OR cancel_date >= ve.signature_date)
                                  AND ROWNUM < 2),
                        1,
                        '1',
                        '0'
                     ),
                     '0'
                  )
                     risky,
                  --
                  il.role_type,
                  cp.surname,
                  pe.tax_number,
                  ip.action_code,
                  get_eh_score(p_contract_id, pe.tax_number) eh_score,
                  is_previously_denied(p_contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
                  il.link_type,
                  NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
                  NVL(pe.is_foreign_company, 0)  is_foreign_company
           FROM   pme_bv_interested_parties ip,
                  pme_bv_ip_links il,
                  bv_koc_ocp_pol_versions_ext ve,
                  cp_partners cp,
                  koc_cp_partners_ext pe
          WHERE       ip.contract_id = p_contract_id
                  AND ip.contract_id = il.contract_id
                  AND ip.ip_no = il.ip_no
                  AND ip.action_code <> 'D'
                  AND ip.partner_id = cp.part_id
                  AND cp.part_id = pe.part_id
                  AND ip.contract_id = ve.contract_id
                  AND il.role_type = 'PH';

      reco_partners        c_partners_ph%ROWTYPE;

      CURSOR c_partitions
      IS
         SELECT -- adress     coefficients     fire mkec extras     fire risk detail
               NVL (cp.net_premium, 0) + NVL (cp.tax_amount, 0)
                     gross_premium_by_partition_tl,
                  -- insured partners     lcr partners
                  NVL (cp.net_premium, 0) net_premium_by_partition_tl,
                  -- notes
                  p.partition_no,
                  p.partition_type,
                  -- policy partition covers
                  NVL (
                     koc_sum_insured.get_sum_ins (p.contract_id,
                                                  v.version_no,
                                                  p.partition_no,
                                                  NULL,
                                                  NULL,
                                                  NULL,
                                                  1,
                                                  v.business_start_date,
                                                  'TL')
                     * pe.def_sum_insured_swf_exchg
                     / koc_curr_utils.retrieve_effective_selling (
                          pe.def_sum_insured_swf,
                          v.business_start_date
                       ),
                     0
                  )
                     sumins_whole_cover_by_risk_tl,           -- default value
                  pe.def_sum_insured_swf swift_code,
                  NVL (pe.def_sum_insured_swf_exchg, 1)
                     def_sum_insured_swf_exchg,               -- default value
                  p.action_code,
                  NVL(pe.has_program_pol , 0) allianz_program_pol ,
                  NVL(pe.has_fronting_pol ,0) allianz_fronting_pol,
               NVL(mn.job_type, 0) job_type
           FROM   pme_bv_partitions p,
                  KOC_OCP_CAR_EAR mn,
                  bv_koc_ocp_partitions_ext pe,
                  (  SELECT   partition_no,
                              SUM(NVL (final_premium, 0)
                                  * NVL (premium_swf_exchg, 1))
                                 net_premium,
                              SUM(NVL (final_tax, 0)
                                  * NVL (premium_swf_exchg, 0))
                                 tax_amount
                       FROM   bv_koc_ocp_policy_covers_ext
                      WHERE   contract_id = p_contract_id
                   GROUP BY   partition_no) cp,
                  pme_bv_policy_versions v
          WHERE       p.contract_id = pe.contract_id
                  AND p.partition_no = pe.partition_no
                  AND p.contract_id = p_contract_id
                  AND p.partition_no = cp.partition_no
                  AND p.contract_id = mn.contract_id
                  AND p.contract_id = v.contract_id;

      reco_partitions      c_partitions%ROWTYPE;

      CURSOR adress_coordinates (
         p_partition_no IN NUMBER
      )
      IS
         SELECT   a.xcoor, a.ycoor
           FROM   bv_koc_ocp_buildings b,
                  koc_cp_address_ext a,
                  cp_addresses c,
                  bv_koc_ocp_pol_versions_ext e
          WHERE       b.contract_id = p_contract_id
                  AND b.partition_no = p_partition_no
                  AND b.risk_add_id = c.add_id
                  AND c.add_id = a.add_id
                  AND b.contract_id = e.contract_id;

      coordinates          adress_coordinates%ROWTYPE;
      v_coordinate_found   NUMBER;
      v_signature_date     DATE;

      CURSOR earthquake_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 2 --deprem riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      earthquakedates      earthquake_dates%ROWTYPE;
      v_default_end_date   DATE;

      CURSOR flood_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 4 --sel riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      flooddates           flood_dates%ROWTYPE;

      CURSOR landslide_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 6 --yer kaymasi riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      landslidedates       landslide_dates%ROWTYPE;

      CURSOR terror_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 3 --teror riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      terrordates          terror_dates%ROWTYPE;

      CURSOR c_in_terror_list
      IS
         SELECT   1
           FROM   bv_koc_ocp_buildings b,
                  koc_cp_address_ext a,
                  pme_bv_policy_versions e,
                  alz_terror_control t,
                  pme_bv_partitions p,
                  pme_bv_policy_contracts pc
          WHERE       b.contract_id = p_contract_id
                  AND b.contract_id = p.contract_id
                  AND b.partition_no = v_partition_no
                  AND b.partition_no = p.partition_no
                  AND b.contract_id = pc.contract_id
                  AND b.risk_add_id = a.add_id
                  AND b.contract_id = e.contract_id
                  AND t.partition_type = p.partition_type
                  AND t.product_id = pc.product_id
                  AND t.city_code = a.city_code
                  AND (t.district_code = a.district_code
                      OR NVL(lower(t.district_code), 'x') = 'x')         -- Ilin butun ilcelerinin otorizasyona dusmesi istendiginde
                  --AND t.priority = 1
                  --AND t.is_industrial = 1
                  AND t.validity_start_date < trunc(sysdate)
                  AND (t.validity_end_date > trunc(sysdate)
                      OR t.validity_end_date IS NULL);

      v_in_terror_list NUMBER(1);

      CURSOR c_address (
         p_earthquake_start_date   IN            DATE,
         p_earthquake_end_date     IN            DATE,
         p_flood_start_date        IN            DATE,
         p_flood_end_date          IN            DATE,
         p_landslide_start_date    IN            DATE,
         p_landslide_end_date         OUT        DATE,
         p_terror_start_date       IN            DATE,
         p_terror_end_date         IN            DATE
      )
      IS
         SELECT   b.risk_add_id,
                  a.city_code,
                  c.country_code,
                  b.cresta_zone_code,
                  a.district_code,
                  p_earthquake_end_date earthquake_auth_end_date,
                  p_earthquake_start_date earthquake_auth_start_date,
                  b.earthquake_zone_code,
                  p_flood_end_date flood_auth_end_date,
                  p_flood_start_date flood_auth_start_date,
                  p_landslide_end_date landslide_end_date,
                  p_landslide_start_date landslide_start_date,
                  b.municipality_code,
                  a.quarter_code,
                  p_terror_end_date terror_end_date,
                  p_terror_start_date terror_start_date,
                  koc_address_utils.address (b.risk_add_id) adres_txt
           FROM   bv_koc_ocp_car_ear b,
                  koc_cp_address_ext a,
                  cp_addresses c,
                  bv_koc_ocp_pol_versions_ext e
          WHERE       b.contract_id = p_contract_id
                  AND b.partition_no = v_partition_no
                  AND b.risk_add_id = c.add_id
                  AND c.add_id = a.add_id
                  AND b.contract_id = e.contract_id;

      reco_address         c_address%ROWTYPE;

      CURSOR find_signature_date
      IS
         SELECT   e.signature_date
           FROM   wip_koc_ocp_pol_versions_ext e
          WHERE   e.contract_id = p_contract_id;

      CURSOR c_cumul (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS                                                                    --
         SELECT   /*+ index ( r KOC_CUMUL_RISK_DEF_IDX2 ) */ 1
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.area_type NOT IN ('10', '9')
                  AND r.risk_type = 1 --kumul
                  AND r.dune_type = 1 --yangin riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      reco_cumul           c_cumul%ROWTYPE;

      CURSOR c_coefficient
      IS                                                                    --
         SELECT   dr.action_code,
                  dr.TYPE cotype,
                  dr.discount_saving_code,
                  dr.exe_amnt_or_rate,                       -- 1 oran 2 tutar
                  dr.exemp_amount_swf,
                  dr.exemption_type,
                  -- 1 sigorta bedeli 2 hasar bedeli 3 ödenen tazminat 4 makina bedeli
                  dr.dim_value exemption_value,
                  NVL (dr.min_exemp_sum_ins, 0) min_exemp_sum_ins, --default value
                  dr.min_exemp_sum_ins_swf,
                  NVL (rate, 0) rate,                         -- default value
                  NVL (dr.is_rate_enter_manually, 0) is_rate_enter_manually, --default value
                  NVL (dr.tariff_disc_rate, 0) tariff_disc_rate, -- default value
                  NVL (dr.document_group_id, 0) document_group_id
           FROM   bv_koc_ocp_disc_save_rates dr                             --
          WHERE   contract_id = p_contract_id
                  AND dr.partition_no = v_partition_no;

      reco_coefficient     c_coefficient%ROWTYPE;



      CURSOR c_activity_subject IS
       SELECT segment, segment_code, subsegment, main_activity, sme_authorization, act_subj_exp
       FROM koc_oc_act_subject_ref r,
            bv_koc_ocp_buildings bld,
            ocp_policy_contracts c,
            bv_koc_ocp_liabilities l
      WHERE bld.contract_id = p_contract_id
        AND bld.partition_no = v_partition_no
        AND bld.contract_id = l.contract_id(+)
        AND bld.partition_no = l.partition_no(+)
        AND c.contract_id = bld.contract_id
        AND r.activity_subject_code = bld.activity_subject_code
        AND (r.validity_start_date <= TRUNC (sysdate)
              AND ( (c.product_id IN (29, 30)
                     AND ( (r.validity_end_date IS NULL
                            AND r.sme_end_date IS NULL)
                          OR ( (r.validity_end_date IS NULL
                                OR r.validity_end_date >= TRUNC (sysdate))
                              AND r.sme_end_date >= TRUNC (sysdate))))
                   OR (c.product_id NOT IN (29, 30)
                       AND (r.validity_end_date >= TRUNC (sysdate)
                            OR r.validity_end_date IS NULL)))
             )
     ;

    reco_activity_subject    c_activity_subject%ROWTYPE;

    CURSOR c_degree_of_risk IS
        SELECT degree_of_risk
          FROM koc_ocp_buildings bld,
               ocp_policy_contracts c,
               koc_oc_act_subject_ref_ext asr,
               ocp_partitions p
         WHERE bld.contract_id = p_contract_id
           AND bld.partition_no = v_partition_no
           AND c.contract_id = bld.contract_id
           AND bld.activity_subject_code = asr.activity_subject_code
           AND c.product_id = asr.product_id
           AND p.contract_id = bld.contract_id
           AND p.partition_no = bld.partition_no
           AND p.partition_type = asr.partition_type
           AND asr.validity_start_date <= TRUNC (sysdate)
           AND (asr.validity_end_date > TRUNC (sysdate)
              OR asr.validity_end_date IS NULL);

    v_degree_of_risk VARCHAR2(25);



    CURSOR c_note
      IS
         SELECT   NVL (a.has_new_notes, 0) has_new_notes,     -- default value
                  NVL (a.u_w_edit, 0) u_w_edit,                 --default value
                  note_type
           FROM   koc_notes_authorization a
          WHERE       ins_obj_uid = TO_CHAR (p_contract_id)
                  AND a.sub_level2 = v_partition_no
                  AND a.sub_level1 < (SELECT   NVL (version_no, 0) + 1
                                        FROM   wip_policy_versions
                                       WHERE   contract_id = wip_contract_id);

      reco_note            c_note%ROWTYPE;

      CURSOR c_cover
      IS
         SELECT   ce.action_code,
                  NVL (ce.adjustment_sum_insured, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1)
                     adjustment_sum_insured_tl, -- default value
                  NVL (c.sum_insured_whole_cover, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1)
                     cover_amount_tl, -- default value
                  c.cover_code,
                  ccd.cover_cat_group,
                  cd.explanation,
                  (NVL (ce.final_premium, 0) + NVL (ce.final_tax, 0))
                  * NVL (ce.premium_swf_exchg, 1)
                     gross_premium_tl,
                  NVL (ce.final_premium, 0) * NVL (ce.premium_swf_exchg, 1)
                     net_premium_tl,
                  NULL swift_code,
                  ce.premium_swf_exchg,
                  c.sum_insured_whole_cover_swf bedel_dvz_cinsi,
                  ce.sum_insured_swf_exchg bedel_kur,
                  c.ftpremium_or_whole_cover_swf prim_dvz_cinsi,
                  ce.premium_swf_exchg prim_kur,
                  0 sigorta_bedeline_dahil, -- current ve prev de önemli degil ,
                  NVL (ce.full_cover, 0) tam_teminat,
                  NVL (ce.comm_rate, 0) comm_rate,
                  NVL (ce.tariff_comm_rate, 0) tariff_comm_rate,
                  NVL (ce.clm_limit_per_year, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1) clm_limit_per_year
           FROM   pme_bv_policy_covers c,
                  bv_koc_ocp_policy_covers_ext ce,
                  koc_v_cover cd,
                  koc_oc_cover_definitions ccd,
                  bv_koc_ocp_pol_versions_ext ve
          WHERE       c.contract_id = p_contract_id
                  AND c.partition_no = v_partition_no
                  AND c.contract_id = ce.contract_id
                  AND c.partition_no = ce.partition_no
                  AND c.cover_no = ce.cover_no
                  AND c.cover_code = ce.cover_code
                  AND c.contract_id = ve.contract_id
                  AND c.cover_code = cd.cover_code
                  AND c.cover_code = ccd.cover_code
                  AND ccd.validity_start_date <= ve.signature_date
                  AND (ccd.validity_end_date IS NULL
                       OR ccd.validity_end_date > ve.signature_date);

      reco_cover           c_cover%ROWTYPE;

      CURSOR c_partners_partition (
         v_role_type VARCHAR2
      )
      IS                                                                    --
         SELECT   DECODE (
                  (select count(*) from koc_cp_blacklist_entries where part_id in (
                    select part_id from koc_cp_partners_ext
                        where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                            AND from_date <= ve.signature_date
                            AND (TO_DATE IS NULL
                                 OR TO_DATE >= ve.signature_date)),
                  0,
                  '0',
                  '1'
               )
                  blacklist,
                 DECODE (
                    is_sisbis(p_contract_id, pe.identity_no, pe.tax_number, il.role_type),
                    0,
                    '0',
                    '1'
                 )
                 sisbis,
                  cp.date_of_birth,
                  DECODE (cp.partner_type, 'P', cp.first_name, cp.NAME)
                     first_name,
                  pe.identity_no,
                  cp.marital_status,
                  cp.nationality,
                  cp.part_id,
                  cp.partner_type,
                  --
                  DECODE (
                     (SELECT   1
                        FROM   koc_risk_partners
                       WHERE   (tax_number = pe.tax_number
                                OR identity_no = pe.identity_no)
                               AND entry_date <= ve.signature_date
                               AND (cancel_date IS NULL
                                    OR cancel_date >= ve.signature_date)
                               AND ROWNUM < 2),
                     1,
                     '1',
                     '0'
                  )
                     risky,
                  --
                  il.role_type,
                  cp.surname,
                  pe.tax_number,
                  ip.action_code,
                  get_eh_score(p_contract_id, pe.tax_number) eh_score,
                  is_previously_denied(p_contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
                  il.link_type,
                  NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
                  NVL(pe.is_foreign_company, 0)  is_foreign_company
           FROM   pme_bv_interested_parties ip,
                  pme_bv_ip_links il,
                  bv_koc_ocp_pol_versions_ext ve,
                  cp_partners cp,
                  koc_cp_partners_ext pe
          WHERE       ip.contract_id = p_contract_id
                  AND ip.contract_id = il.contract_id
                  AND il.partition_no = v_partition_no
                  AND ip.ip_no = il.ip_no
                  AND ip.action_code <> 'D'
                  AND ip.partner_id = cp.part_id
                  AND cp.part_id = pe.part_id
                  AND ip.contract_id = ve.contract_id
                  AND il.role_type = v_role_type;                       --'PH'


   -------------------------------------------------------------------------------


       CURSOR c_fiziksel_bolge IS             -- Fiziksel Bölge Bilgisi eklendi. Kasko icin Tolga objeye eklemek gerekecek
        SELECT 1
          FROM KOC_DMT_REGION_CODE_REF r, koc_dmt_v_current_agents d, ocp_policy_bases o
         WHERE o.contract_id = p_contract_id
           AND d.AGEN_INT_ID = o.AGENT_ROLE
           AND r.REGION_CODE= d.region_code
           AND r.PHY_REGION_CODE <> '55';

           v_fiziksel_bolge number(1);

       CURSOR c_branch IS
        SELECT B.BRANCH_EXT_REF
         FROM koc_ocp_pol_versions_ext a,koc_ocp_pol_contracts_ext b
        WHERE a.CONTRACT_ID = p_contract_id
         AND  b.CONTRACT_ID = a.CONTRACT_ID;

         v_branch_xref varchar2(10);


   v_is_payment_rate_changed number;

   BEGIN
      -- version_no set
      BEGIN
         SELECT   version_no
           INTO   v_version_no
           FROM   ocp_policy_versions
          WHERE   contract_id = p_contract_id AND top_indicator = 'Y';
      EXCEPTION
         WHEN OTHERS
         THEN
            p_ocp_prv_policy := NULL;
      END;

      pme_api.set_query_version (v_version_no);

      OPEN c_policy;

      FETCH c_policy INTO reco_policy;

      IF c_policy%NOTFOUND
      THEN
         CLOSE c_policy;

         p_ocp_prv_policy := NULL;
         RETURN;
      END IF;

      CLOSE c_policy;


      OPEN c_agent;
      FETCH c_agent INTO reco_agent;
      CLOSE c_agent;


       OPEN c_fiziksel_bolge;
       FETCH c_fiziksel_bolge INTO v_fiziksel_bolge;

        IF c_fiziksel_bolge%NOTFOUND
        THEN
          v_fiziksel_bolge :=0;
        END IF;
        CLOSE c_fiziksel_bolge;

        OPEN c_branch;
        FETCH c_branch INTO v_branch_xref;
        CLOSE c_branch;

      --banka YKB
      if reco_agent.code='10503' Then
          reco_policy.contract_type :=0;
      end if;

      v_is_payment_rate_changed := is_payment_rate_changed(p_contract_id, reco_policy.product_id);
      p_ocp_prv_policy :=
         CAREAR_BRE_policy (reco_policy.contract_id,
                      reco_policy.version_no,
                      reco_policy.policy_type,
                      reco_policy.term_start_date,
                      reco_policy.signature_date,
                      reco_policy.term_end_date,
                      reco_policy.product_id,
                      reco_policy.business_start_date,
                      reco_policy.group_code,
                      NULL,                                   -- CAREAR_BRE_agent
                      NULL,                                    -- CAREAR_BRE_user
                      reco_policy.endorsement_no,
                      reco_policy.contract_type,
                      reco_policy.reference_contract_id,
                      reco_policy.oip_policy_ref,
                      reco_policy.gross_premium_tl,
                      reco_policy.usd_exchg_rate,
                      reco_policy.eur_exchg_rate,
                      reco_policy.net_premium_tl,
                      reco_policy.is_renewal,
                      reco_policy.old_contract_id,
                      reco_policy.pol_signature_date,
                      reco_policy.is_industrial,
                      NULL,
                      NULL,
                      NULL,
                      reco_policy.is_date_fixed,
                      reco_policy.aff_payment_type,
                      reco_policy.bank_sales_channel,
                      reco_policy.fronting_agent_comm_rate,
                      v_branch_xref,
                      v_fiziksel_bolge,
                      reco_policy.endors_reason_code,
                      reco_policy.adj_gross_premium_tl,
                      reco_policy.bsmv_tax,
                      reco_policy.downpayment_rate,    --peþinat
                      0, --reco_policy.sum_imsured_total,   --
                      reco_policy.direkt_policy_type,       --direk endirek
                      reco_policy.is_reinsured,
                      v_is_payment_rate_changed
                     );




   IF nvl(reco_agent.int_id,0) = 0 THEN
      NULL;
   ELSE
         p_ocp_prv_policy.AGENT :=
            CAREAR_BRE_agent (reco_agent.int_id,
                           reco_agent.code,
                           reco_agent.sales_channel,
                           reco_agent.main_group,
                           reco_agent.sub_group,
                           reco_agent.category_type,
                           reco_agent.region_code);
   END IF;


      ---<--- TYH-66927 IDM Entegrasyonu -----
      -- user / role
      /*OPEN c_user;

      FETCH c_user INTO reco_user;

      IF c_user%NOTFOUND
      THEN
         NULL;
      ELSE
         p_ocp_prv_policy.carear_user :=
            CAREAR_BRE_user (reco_user.username,
                          reco_user.first_name,
                          reco_user.surname,
                          NULL,
                          reco_user.user_type,
                          reco_user.user_level);
         v_count := 0;
         p_ocp_prv_policy.carear_user.role_code := CAREAR_BRE_role_table ();

         OPEN c_role (reco_user.username);

         LOOP
            FETCH c_role INTO reco_role;

            EXIT WHEN c_role%NOTFOUND;
            v_count := v_count + 1;
            p_ocp_prv_policy.carear_user.role_code.EXTEND;
            p_ocp_prv_policy.carear_user.role_code (v_count) :=
               CAREAR_BRE_role (reco_role.role_code);
         END LOOP;

         CLOSE c_role;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.carear_user.role_code := NULL;
         END IF;
      END IF;

      CLOSE c_user;*/

      vv_username := NULL;
      vv_first_name := NULL;
      vv_surname := NULL;
      vv_user_type := NULL;

      open c_pme_user;
      fetch c_pme_user into vv_username;
      close c_pme_user;

      alz_base_function_utils.user_type(vv_username, vv_first_name, vv_surname, vv_user_type);

      IF vv_first_name IS NOT NULL THEN
         vv_user_level := get_user_level(vv_username); -- TYH-66927 IDM Entegrasyonu
         p_ocp_prv_policy.carear_user :=
            CAREAR_BRE_user (vv_username,
                          vv_first_name,
                          vv_surname,
                          NULL,
                          vv_user_type,
                          vv_user_level);
         v_count := 0;
         p_ocp_prv_policy.carear_user.role_code := CAREAR_BRE_role_table ();

         OPEN c_role (vv_username);

         LOOP
            FETCH c_role INTO reco_role;

            EXIT WHEN c_role%NOTFOUND;
            v_count := v_count + 1;
            p_ocp_prv_policy.carear_user.role_code.EXTEND;
            p_ocp_prv_policy.carear_user.role_code (v_count) :=
               CAREAR_BRE_role (reco_role.role_code);
         END LOOP;

         CLOSE c_role;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.carear_user.role_code := NULL;
         END IF;
      END IF;
      --->------------------------------------

      --
      v_count := 0;
      p_ocp_prv_policy.policy_holders := CAREAR_BRE_partner_table ();

      OPEN c_partners_ph;

      LOOP
         FETCH c_partners_ph INTO reco_partners;

         EXIT WHEN c_partners_ph%NOTFOUND;
         v_count := v_count + 1;
         p_ocp_prv_policy.policy_holders.EXTEND;
         p_ocp_prv_policy.policy_holders (v_count) :=
            CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
      END LOOP;

      CLOSE c_partners_ph;

      IF v_count = 0
      THEN
         p_ocp_prv_policy.policy_holders := NULL;
      END IF;

      OPEN find_signature_date;
      FETCH find_signature_date INTO v_signature_date;
      CLOSE find_signature_date;

      v_count := 0;
      p_ocp_prv_policy.policy_partitions := CAREAR_BRE_partition_table ();

      OPEN c_partitions;

      LOOP
         FETCH c_partitions INTO reco_partitions;

         EXIT WHEN c_partitions%NOTFOUND;
         v_partition_no := reco_partitions.partition_no;
         v_count := v_count + 1;
         p_ocp_prv_policy.policy_partitions.EXTEND;
         p_ocp_prv_policy.policy_partitions (v_count) :=
            CAREAR_BRE_partition (reco_partitions.action_code,
                            reco_partitions.partition_no,
                            reco_partitions.partition_type,
                            reco_partitions.sumins_whole_cover_by_risk_tl,
                            reco_partitions.net_premium_by_partition_tl,
                            reco_partitions.gross_premium_by_partition_tl,
                            reco_partitions.swift_code,
                            reco_partitions.def_sum_insured_swf_exchg,
                            NULL,                                    -- covers
                            NULL,                                     -- notes
                            NULL,                              -- coefficients
                            NULL,                              -- lcr_partners
                            NULL,                           --insured_partners
                            NULL,                           -- address
                            NULL,                                   --clauses
                            NULL,                          -- activity_subject
                            NULL,                           -- quote_same_quarter
                            reco_partitions.allianz_program_pol ,
                            reco_partitions.allianz_fronting_pol ,
                            reco_partitions.job_type );

         OPEN adress_coordinates (v_partition_no);
         FETCH adress_coordinates INTO coordinates;
         IF adress_coordinates%NOTFOUND THEN
             coordinates := NULL;
         END IF;
         CLOSE adress_coordinates;

         --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri aliniyor
         IF coordinates.xcoor IS NOT NULL AND coordinates.ycoor IS NOT NULL
         THEN
            v_coordinate_found := 1;

            OPEN earthquake_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH earthquake_dates INTO earthquakedates;
            IF earthquake_dates%NOTFOUND THEN
                 earthquakedates := NULL;
            END IF;
            CLOSE earthquake_dates;

            OPEN flood_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH flood_dates INTO flooddates;
            IF flood_dates%NOTFOUND THEN
                 flooddates := NULL;
            END IF;
            CLOSE flood_dates;

            OPEN landslide_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH landslide_dates INTO landslidedates;
            IF landslide_dates%NOTFOUND THEN
                 landslidedates := NULL;
            END IF;
            CLOSE landslide_dates;

            OPEN terror_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH terror_dates INTO terrordates;
            IF (terror_dates%NOTFOUND OR terrordates.validity_end_date < sysdate)THEN
                 OPEN c_in_terror_list;
                 FETCH c_in_terror_list INTO v_in_terror_list;
                 IF c_in_terror_list%NOTFOUND THEN
                   terrordates := NULL;
                 ELSE -- Teror il/ilce listesindeyse start/end date'i otorizasyona dusecek sekilde set et
                   terrordates.validity_start_date := sysdate-2;
                   terrordates.validity_end_date := sysdate+2;
                 END IF;
                 CLOSE c_in_terror_list;
            END IF;
            CLOSE terror_dates;
         END IF;
         --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri alindi


         IF v_coordinate_found = 1
         THEN
            OPEN c_address (earthquakedates.validity_start_date,
                            earthquakedates.validity_end_date,
                            flooddates.validity_start_date,
                            flooddates.validity_end_date,
                            landslidedates.validity_start_date,
                            landslidedates.validity_end_date,
                            terrordates.validity_start_date,
                            terrordates.validity_end_date);
         ELSE
            OPEN c_address (v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date);
         END IF;

         FETCH c_address INTO reco_address;

         IF c_address%NOTFOUND
         THEN
            NULL;
         ELSE
            p_ocp_prv_policy.policy_partitions (v_count).address :=
               CAREAR_BRE_address (
                  CAREAR_BRE_basic_address (reco_address.risk_add_id,
                                         reco_address.country_code,
                                         reco_address.city_code,
                                         reco_address.district_code,
                                         reco_address.quarter_code,
                                         reco_address.adres_txt),
                  reco_address.municipality_code,
                  reco_address.earthquake_zone_code,
                  reco_address.cresta_zone_code,
                  reco_address.earthquake_auth_start_date,
                  reco_address.earthquake_auth_end_date,
                  reco_address.flood_auth_start_date,
                  reco_address.flood_auth_end_date,
                  -- landslide mahalle bazyna kadar inebilmekte  ilçede olmayyp mahallede olabilir eklenmesi gerekir.
                  reco_address.landslide_start_date,
                  reco_address.landslide_end_date,
                  reco_address.terror_start_date,
                  reco_address.terror_end_date,
                  NULL                                                 --cumul
               );

            IF coordinates.xcoor IS NOT NULL
               AND coordinates.ycoor IS NOT NULL
            THEN
               OPEN c_cumul (coordinates.xcoor, coordinates.ycoor);

               FETCH c_cumul INTO reco_cumul;

               IF c_cumul%NOTFOUND
               THEN
                  NULL;
               ELSE
                  p_ocp_prv_policy.policy_partitions (v_count).address.cumul :=
                     CAREAR_BRE_cumul (1, 'H', NULL);
               END IF;

               CLOSE c_cumul;
            ELSE
               p_ocp_prv_policy.policy_partitions (v_count).address.cumul :=
                  CAREAR_BRE_cumul (1, 'H', NULL);
            END IF;
         END IF;

         CLOSE c_address;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).coefficients :=
            CAREAR_BRE_coef_table ();

         OPEN c_coefficient;

         LOOP
            FETCH c_coefficient INTO reco_coefficient;

            EXIT WHEN c_coefficient%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).coefficients.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).coefficients (v_count2) :=
               CAREAR_BRE_coefficient (reco_coefficient.discount_saving_code, -- coefficientCode
                                    reco_coefficient.cotype,
                                    reco_coefficient.action_code,
                                    reco_coefficient.rate,
                                    reco_coefficient.exemption_value,
                                    reco_coefficient.exemption_type,
                                    reco_coefficient.is_rate_enter_manually,
                                    reco_coefficient.min_exemp_sum_ins,
                                    reco_coefficient.min_exemp_sum_ins_swf,
                                    reco_coefficient.exemp_amount_swf,
                                    reco_coefficient.tariff_disc_rate,
                                    reco_coefficient.exe_amnt_or_rate,
                                    reco_coefficient.document_group_id);
         END LOOP;

         CLOSE c_coefficient;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).coefficients := NULL;
         END IF;






         OPEN c_activity_subject;

         FETCH c_activity_subject INTO reco_activity_subject;

         IF c_activity_subject%NOTFOUND
         THEN
            NULL;
         ELSE
            OPEN c_degree_of_risk;

            FETCH c_degree_of_risk INTO v_degree_of_risk;

            CLOSE c_degree_of_risk;

            p_ocp_prv_policy.policy_partitions (v_count).activity_subject :=
               CAREAR_BRE_activity_subject (reco_activity_subject.segment,
                                        reco_activity_subject.segment_code,
                                        reco_activity_subject.subsegment,
                                        reco_activity_subject.main_activity,
                                        NVL(v_degree_of_risk, ' '),
                                        reco_activity_subject.act_subj_exp
                                        );
         END IF;

         CLOSE c_activity_subject;



         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).notes :=
            CAREAR_BRE_note_table ();

         OPEN c_note;

         LOOP
            FETCH c_note INTO reco_note;

            EXIT WHEN c_note%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).notes.EXTEND;
            p_ocp_prv_policy.policy_partitions (v_count).notes (v_count2) :=
               CAREAR_BRE_note (reco_note.has_new_notes, reco_note.u_w_edit, reco_note.note_type);
         END LOOP;

         CLOSE c_note;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).notes := NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).covers :=
            CAREAR_BRE_cover_table ();

         OPEN c_cover;

         LOOP
            FETCH c_cover INTO reco_cover;

            EXIT WHEN c_cover%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).covers.EXTEND;
            p_ocp_prv_policy.policy_partitions (v_count).covers (v_count2) :=
               CAREAR_BRE_cover (reco_cover.cover_code,
                           reco_cover.explanation,
                           --cover_type              ,
                           reco_cover.cover_cat_group,
                           reco_cover.cover_amount_tl,
                           reco_cover.adjustment_sum_insured_tl,
                           reco_cover.net_premium_tl,
                           reco_cover.gross_premium_tl,
                           reco_cover.action_code,
                           reco_cover.bedel_dvz_cinsi,
                           reco_cover.bedel_kur,
                           reco_cover.prim_dvz_cinsi,
                           reco_cover.prim_kur,
                           reco_cover.sigorta_bedeline_dahil,
                           reco_cover.tam_teminat,
                           reco_cover.comm_rate,
                           reco_cover.tariff_comm_rate,
                           reco_cover.clm_limit_per_year
                           );
         END LOOP;

         CLOSE c_cover;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).covers := NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).insured_partners :=
            CAREAR_BRE_partner_table ();

         OPEN c_partners_partition ('INS');

         LOOP
            FETCH c_partners_partition INTO reco_partners;

            EXIT WHEN c_partners_partition%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).insured_partners.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).insured_partners (v_count2) :=
               CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
         END LOOP;

         CLOSE c_partners_partition;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).insured_partners :=
               NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).lcr_partners :=
            CAREAR_BRE_partner_table ();

         OPEN c_partners_partition ('LCR');

         LOOP
            FETCH c_partners_partition INTO reco_partners;

            EXIT WHEN c_partners_partition%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).lcr_partners.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).lcr_partners (v_count2) :=
               CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
         END LOOP;

         CLOSE c_partners_partition;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).lcr_partners := NULL;
         END IF;
      END LOOP;

      CLOSE c_partitions;

      IF v_count = 0
      THEN
         p_ocp_prv_policy.policy_partitions := NULL;
      END IF;

      -- önceki poliçe ve hasarlar , ocp ve prev için kullanilmiyor
      p_ocp_prv_policy.prev_carear_claims := NULL;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         alz_web_process_utils.process_result (
            0,
            9,
            -1,
            'INVALID_DATA',
            'OCP/PREV HATA',
            'OCP/PREV HATA',
            NULL,
            NULL,
            'alz_carear_bre_utils , p_OCP_PRV',
            NULL,
            p_process_results
         );
   END p_ocp_prv;

   PROCEDURE p_ocq_prv (
      p_quote_id              NUMBER,
      wip_contract_id         NUMBER,            -- cursor c_note için gerekli
      p_ocp_prv_policy    OUT CAREAR_BRE_policy,
      p_process_results   OUT customer.process_result_table
   )
   IS
      v_count              INT;
      v_count2             INT;
      v_partition_no       NUMBER (5);
      v_contract_id        NUMBER (10);
      v_version_no         NUMBER (5);
      v_found_factor              NUMBER (1);
      v_found                     NUMBER (1);



      CURSOR c_policy
      IS
         SELECT   b.contract_id,
                  NVL (v.version_no, 0) version_no, -- default value
                  DECODE (bre_find_old_contract_id (wip_contract_id), NULL, '0', '1')
                     policy_type,
                  b.term_start_date,
                  ve.signature_date,
                  b.term_end_date,
                  v.product_id,
                  b.term_start_Date business_start_date,
                  ve.group_code,
                  NVL (ve.endorsement_no, 0) endorsement_no,  -- DEFAULT VALUE
                  (CASE
                      WHEN c.contract_type = 0 AND c.prev_quote_ref IS NULL
                      THEN
                         2
                      WHEN c.contract_type = 0
                           AND c.prev_quote_ref IS NOT NULL
                      THEN
                         3
                      WHEN c.contract_type = 1
                           AND v.movement_reason_code <> 'ENBQ'
                      THEN
                         0
                      WHEN c.contract_type = 1
                           AND v.movement_reason_code = 'ENBQ'
                      THEN
                         1
                   END)
                     contract_type, --0=Poliçe, 1=Teklife Istinaden Poliçe ,2=Teklif, 3=Teklife Istinaden Teklif
                  NVL (c.reference_contract_id, 0) reference_contract_id, -- DEFAULT VALUE
                  c.oip_policy_ref,
                    NVL (v.full_term_premium, 0)
                  + NVL (ve.fire_tax, 0)
                  + NVL (ve.insur_process_tax, 0)
                     gross_premium_tl,
                     (  NVL (v.adjustment_premium, 0)
                   + NVL (ve.adj_insur_process_tax, 0)
                   + NVL (ve.adj_traffic_fund, 0)
                   + NVL (ve.adj_guarantee_fund, 0)
                   + NVL (ve.adj_fire_tax, 0))
                     adj_gross_premium_tl,
                  NVL (
                     koc_curr_utils.retrieve_currency_buying (
                        'USD',
                        b.term_start_date
                     ),
                     1
                  )
                     usd_exchg_rate,                          -- DEFAULT VALUE
                  NVL (
                     koc_curr_utils.retrieve_currency_buying (
                        'EUR',
                        b.term_start_date
                     ),
                     1
                  )
                     eur_exchg_rate,                          -- DEFAULT VALUE
                  NVL (v.full_term_premium, 0) net_premium_tl, --default value
                  NVL (c.is_renewal, 0) is_renewal,           -- default value
                  NVL (c.old_contract_id, 0) old_contract_id, -- default value
                  ve.signature_date pol_signature_date,
                  NVL(ve.is_industrial , 0) is_industrial,
                  NVL (ve.AFF_PAYMENT_TYPE, '0') AFF_PAYMENT_TYPE,
                  NVL (ve.IS_DATE_FIXED, 0) is_date_fixed,
                  NVL (ve.bank_sales_channel, '0') bank_sales_channel,
                  NVL (c.fronting_agent_comm_rate, 0) fronting_agent_comm_rate, --modular
                  NVL (ve.insur_process_tax, 0) bsmv_tax,
                  NVL (c.policy_type,0) direkt_policy_type,
                  ve.endors_reason_code,
                  NVL ((SELECT rate
                          FROM ocq_koc_ocp_payment_plan
                         WHERE contract_id = p_quote_id
                               AND version_no is null
                               AND rownum = 1), 0) downpayment_rate,
                  NVL((select aa.ceding_pct
                       from alz_rci_rate_details aa
                      where aa.rates_Set_id in (
                                                 select a.rates_set_id
                                                   from alz_rci_pol_rate_master  a
                                                  where a.contract_id  = p_quote_id
                                                    and a.partition_no = 1
                                                  --  and a.reins_cover_group = 174     --Gelen deðer 100 deðilse otorizasyona düþer
                                                    and a.top_indicator = 'Y'
                                                )   and arrangement_id = 374
                  AND rownum = 1 ) , 0 )is_reinsured
           FROM   ocq_policy_bases b,
                  ocq_koc_ocp_pol_versions_ext ve,
                  ocq_koc_ocp_pol_contracts_ext c,
                  koc_dmt_agents_ext de,
                  koc_mis_agent_group_ref agr,
                  ocq_quotes v,
                  dmt_agents da
          WHERE       b.quote_id = ve.quote_id
                  AND b.quote_id = c.quote_id
                  AND b.quote_id = p_quote_id
                  AND b.agent_role = de.int_id
                  AND de.mis_main_group = agr.mis_main_group
                  AND de.mis_sub_group = agr.mis_sub_group
                  AND b.quote_id = v.quote_id
                  AND de.int_id = da.int_id;

      reco_policy          c_policy%ROWTYPE;

      CURSOR c_agent
      IS
         SELECT   c.agent_category_type category_type,
                  b.reference_code code,
                  b.int_id,
                  c.mis_main_group main_group,
                  d.explanation sales_channel,
                  c.mis_sub_group sub_group,
                  ve.region_code
           FROM   ocq_policy_bases a,
                  dmt_agents b,
                  koc_dmt_agents_ext c,
                  koc_mis_agent_group_ref d,
                  ocq_koc_ocp_pol_versions_ext ve
          WHERE       a.quote_id = p_quote_id
                  AND a.quote_id = ve.quote_id
                  AND a.agent_role = b.int_id
                  AND b.int_id = c.int_id
                  AND c.mis_main_group = d.mis_main_group
                  AND c.mis_sub_group = d.mis_sub_group;

      reco_agent           c_agent%ROWTYPE;

      ---<--- TYH-66927 IDM Entegrasyonu -----
      -- user ,
      /*CURSOR c_user
      IS
         SELECT   v.username,
                  par.first_name,
                  par.surname,
                  ext.TYPE user_type,
                  get_user_level(v.username) user_level
           FROM   ocq_quotes v,                                   --OCQ_QUOTES
                  koc_cp_partners_ext cp,
                  cp_partners par,
                  koc_v_sec_system_users users,
                  koc_dmt_agents_ext ext
          WHERE       v.quote_id = p_quote_id
                  AND v.username = users.oracle_username
                  AND cp.part_id = users.customer_partner_id
                  AND cp.part_id = par.part_id
                  AND ext.int_id = cp.agen_int_id;

      reco_user            c_user%ROWTYPE;*/


      CURSOR c_quote_user
      IS
         SELECT v.username
           FROM ocq_quotes v
          WHERE v.quote_id = p_quote_id;

      vv_username         VARCHAR2(500);
      vv_first_name     VARCHAR2(500);
      vv_surname         VARCHAR2(500);
      vv_user_type         VARCHAR2(10);
      vv_user_level     NUMBER;
      --->------------------------------------

      -- role ,
      CURSOR c_role (
         p_user VARCHAR2
      )
      IS
         SELECT   role_code
           FROM   koc_auth_user_role_rel a, ocq_koc_ocp_pol_versions_ext v
          WHERE       username = p_user
                  AND v.quote_id = p_quote_id
                  AND a.validity_start_date <= v.signature_date
                  AND (a.validity_end_date IS NULL
                       OR a.validity_end_date >= v.signature_date)
                       ORDER BY A.ROLE_CODE ASC;

      reco_role            c_role%ROWTYPE;

      CURSOR c_partners_ph
      IS
         SELECT   DECODE (
                    (select count(*) from koc_cp_blacklist_entries where part_id in (
                        select part_id from koc_cp_partners_ext
                            where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                                AND from_date <= ve.signature_date
                                AND (TO_DATE IS NULL
                                    OR TO_DATE >= ve.signature_date)),
                        0,
                        '0',
                        '1'
                     )
                     blacklist,
                  DECODE (
                     is_sisbis(ve.contract_id, pe.identity_no, pe.tax_number, il.role_type),
                     0,
                     '0',
                     '1'
                  )
                  sisbis,
                  cp.date_of_birth,
                  DECODE (cp.partner_type, 'P', cp.first_name, cp.NAME)
                     first_name,
                  pe.identity_no,
                  cp.marital_status,
                  cp.nationality,
                  cp.part_id,
                  cp.partner_type,
                  --
                  NVL (
                     DECODE (
                        (SELECT   1
                           FROM   koc_risk_partners
                          WHERE   (tax_number = pe.tax_number
                                   OR identity_no = pe.identity_no)
                                  AND entry_date <= ve.signature_date
                                  AND (cancel_date IS NULL
                                       OR cancel_date >= ve.signature_date)
                                  AND ROWNUM < 2),
                        1,
                        '1',
                        '0'
                     ),
                     '0'
                  )
                     risky,
                  --
                  il.role_type,
                  cp.surname,
                  pe.tax_number,
                  ip.action_code,
                  get_eh_score(wip_contract_id, pe.tax_number) eh_score,
                  is_previously_denied(wip_contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
                  il.link_type,
                  NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
                  NVL(pe.is_foreign_company, 0)  is_foreign_company
           FROM   ocq_interested_parties ip,
                  ocq_ip_links il,
                  ocq_koc_ocp_pol_versions_ext ve,
                  cp_partners cp,
                  koc_cp_partners_ext pe
          WHERE       ip.quote_id = p_quote_id
                  AND ip.quote_id = il.quote_id
                  AND ip.ip_no = il.ip_no
                  AND ip.action_code <> 'D'
                  AND ip.partner_id = cp.part_id
                  AND cp.part_id = pe.part_id
                  AND ip.quote_id = ve.quote_id
                  AND il.role_type = 'PH';

      reco_partners        c_partners_ph%ROWTYPE;

      CURSOR c_partitions
      IS
         SELECT -- adress     coefficients     fire mkec extras     fire risk detail
               NVL (cp.net_premium, 0) + NVL (cp.tax_amount, 0)
                     gross_premium_by_partition_tl,
                  -- insured partners     lcr partners
                  NVL (cp.net_premium, 0) net_premium_by_partition_tl,
                  -- notes
                  p.partition_no,
                  p.partition_type,
                  -- policy partition covers
                  NVL (
                     koc_sum_insured.get_sum_ins (p.contract_id,
                                                  v.version_no,
                                                  p.partition_no,
                                                  NULL,
                                                  NULL,
                                                  NULL,
                                                  1,
                                                  v.effective_date,
                                                  'TL')
                     * pe.def_sum_insured_swf_exchg
                     / koc_curr_utils.retrieve_effective_selling (
                          pe.def_sum_insured_swf,
                          v.effective_date
                       ),
                     0
                  )
                     sumins_whole_cover_by_risk_tl,           -- default value
                  pe.def_sum_insured_swf swift_code,
                  NVL (pe.def_sum_insured_swf_exchg, 1)
                     def_sum_insured_swf_exchg,               -- default value
                  p.action_code,
                  NVL(pe.has_program_pol, 0) allianz_program_pol,
                  NVL(pe.has_fronting_pol ,0) allianz_fronting_pol,
                  NVL(mn.job_type, 0) job_type
           FROM   ocq_partitions p,
                  OCQ_KOC_OCP_CAR_EAR mn,
                  ocq_koc_ocp_partitions_ext pe,
                  (  SELECT   partition_no,
                              SUM(NVL (final_premium, 0)
                                  * NVL (premium_swf_exchg, 1))
                                 net_premium,
                              SUM(NVL (final_tax, 0)
                                  * NVL (premium_swf_exchg, 0))
                                 tax_amount
                       FROM   ocq_koc_ocp_policy_covers_ext
                      WHERE   quote_id = p_quote_id
                   GROUP BY   partition_no) cp,
                  ocq_quotes v
          WHERE       p.quote_id = pe.quote_id
                  AND p.partition_no = pe.partition_no
                  AND p.quote_id = p_quote_id
                  AND p.quote_id = mn.quote_id
                  AND p.partition_no = cp.partition_no
                  AND p.quote_id = v.quote_id;

      reco_partitions      c_partitions%ROWTYPE;

      CURSOR adress_coordinates (
         p_partition_no IN NUMBER
      )
      IS
         SELECT   a.xcoor, a.ycoor
           FROM   ocq_koc_ocp_buildings b,
                  koc_cp_address_ext a,
                  cp_addresses c,
                  ocq_koc_ocp_pol_versions_ext e
          WHERE       b.quote_id = p_quote_id
                  AND b.partition_no = p_partition_no
                  AND b.risk_add_id = c.add_id
                  AND c.add_id = a.add_id
                  AND b.quote_id = e.quote_id;

      coordinates          adress_coordinates%ROWTYPE;
      v_coordinate_found   NUMBER;
      v_signature_date     DATE;

      CURSOR earthquake_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 2 --deprem riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      earthquakedates      earthquake_dates%ROWTYPE;
      v_default_end_date   DATE;

      CURSOR flood_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 4 --sel riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';


      flooddates           flood_dates%ROWTYPE;

      CURSOR landslide_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 6 --yer kaymasi riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      landslidedates       landslide_dates%ROWTYPE;

      CURSOR terror_dates (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS
         SELECT   r.validity_start_date, r.validity_end_date
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.risk_type = 2 --is kabul haddi
                  AND r.dune_type = 3 --teror riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      terrordates          terror_dates%ROWTYPE;

      CURSOR c_in_terror_list
       IS
        SELECT   1
          FROM   ocq_koc_ocp_buildings b,
                 koc_cp_address_ext a,
                 ocq_koc_ocp_pol_versions_ext e,
                 alz_terror_control t,
                 ocq_partitions p,
                 ocq_quotes q
         WHERE    b.quote_id = p_quote_id
                 AND b.quote_id = p.quote_id
                 AND b.partition_no = v_partition_no
                 AND b.partition_no = p.partition_no
                 AND b.quote_id = q.quote_id
                 AND b.risk_add_id = a.add_id
                 AND b.quote_id = e.quote_id
                 AND t.partition_type = p.partition_type
                 AND t.product_id = q.product_id
                 AND t.city_code = a.city_code
                 AND (t.district_code = a.district_code
                     OR NVL(lower(t.district_code), 'x') = 'x')         -- Ilin butun ilcelerinin otorizasyona dusmesi istendiginde
                 --AND t.priority = 1
                 --AND t.is_industrial = 1
                 AND t.validity_start_date < trunc(sysdate)
                 AND (t.validity_end_date > trunc(sysdate)
                     OR t.validity_end_date IS NULL);

      v_in_terror_list NUMBER(1);

      CURSOR c_address (
         p_earthquake_start_date   IN            DATE,
         p_earthquake_end_date     IN            DATE,
         p_flood_start_date        IN            DATE,
         p_flood_end_date          IN            DATE,
         p_landslide_start_date    IN            DATE,
         p_landslide_end_date         OUT        DATE,
         p_terror_start_date       IN            DATE,
         p_terror_end_date         IN            DATE
      )
      IS
         SELECT   b.risk_add_id,
                  a.city_code,
                  c.country_code,
                  b.cresta_zone_code,
                  a.district_code,
                  p_earthquake_end_date earthquake_auth_end_date,
                  p_earthquake_start_date earthquake_auth_start_date,
                  b.earthquake_zone_code,
                  p_flood_end_date flood_auth_end_date,
                  p_flood_start_date flood_auth_start_date,
                  p_landslide_end_date landslide_end_date,
                  p_landslide_start_date landslide_start_date,
                  b.municipality_code,
                  a.quarter_code,
                  p_terror_end_date terror_end_date,
                  p_terror_start_date terror_start_date,
                  koc_address_utils.address (b.risk_add_id) adres_txt
           FROM   ocq_koc_ocp_car_ear b,
                  koc_cp_address_ext a,
                  cp_addresses c,
                  ocq_koc_ocp_pol_versions_ext e
          WHERE       b.quote_id = p_quote_id
                  AND b.partition_no = v_partition_no
                  AND b.risk_add_id = c.add_id
                  AND c.add_id = a.add_id
                  AND b.quote_id = e.quote_id;

      reco_address         c_address%ROWTYPE;

      CURSOR find_signature_date
      IS
         SELECT   e.signature_date
           FROM   wip_koc_ocp_pol_versions_ext e
          WHERE   e.contract_id = wip_contract_id;

      CURSOR c_cumul (
         p_xcoor   IN            NUMBER,
         p_ycoor   IN            NUMBER
      )
      IS                                                                    --
         SELECT   /*+ index ( r KOC_CUMUL_RISK_DEF_IDX2 ) */ 1
           FROM   koc_cumul_risk_details a, koc_cumul_risk_def r
          WHERE       a.shape_id = r.shape_id
                  AND r.area_type NOT IN ('10', '9')
                  AND r.risk_type = 1 --kumul
                  AND r.dune_type = 1 --yangin riski
                  AND r.auth_type = 2 --otorizasyona duser
                  AND a.validity_start_date <= sysdate
                  AND a.validity_end_date >= sysdate
                  AND r.validity_start_date <= sysdate
                  AND r.validity_end_date >= sysdate
                  AND SDO_RELATE(
                        a.coordinates,
                        (sdo_geometry (2001, 8307, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )), --unsalb (sdo_geometry (2001, NULL, sdo_point_type (p_xcoor, p_ycoor, NULL), NULL, NULL )),
                        'mask=anyinteract'
                      )='TRUE';

      reco_cumul           c_cumul%ROWTYPE;

      CURSOR c_coefficient
      IS                                                                    --
         SELECT   dr.action_code,
                  dr.TYPE cotype,
                  dr.discount_saving_code,
                  dr.exe_amnt_or_rate,                       -- 1 oran 2 tutar
                  dr.exemp_amount_swf,
                  dr.exemption_type, -- 1 sigorta bedeli 2 hasar bedeli 3 ödenen tazminat 4 makina bedeli
                  dr.dim_value exemption_value,
                  NVL (dr.min_exemp_sum_ins, 0) min_exemp_sum_ins, --default value
                  dr.min_exemp_sum_ins_swf,
                  NVL (rate, 0) rate,                         -- default value
                  NVL (dr.is_rate_enter_manually, 0) is_rate_enter_manually, --default value
                  NVL (dr.tariff_disc_rate, 0) tariff_disc_rate, -- default value
                  NVL (dr.document_group_id, 0) document_group_id
           FROM   ocq_koc_ocp_disc_save_rates dr                            --
          WHERE   quote_id = p_quote_id AND dr.partition_no = v_partition_no;

      reco_coefficient     c_coefficient%ROWTYPE;



      CURSOR c_activity_subject IS
         SELECT segment, segment_code, subsegment, main_activity, sme_authorization, act_subj_exp
         FROM koc_oc_act_subject_ref r,
              ocq_koc_ocp_buildings bld,
              ocq_quotes q,
              ocq_koc_ocp_liabilities l
        WHERE bld.quote_id = p_quote_id
          AND bld.partition_no = v_partition_no
          AND bld.quote_id = l.quote_id(+)
          AND bld.partition_no = l.partition_no(+)
          AND q.quote_id = bld.quote_id
          AND bld.activity_subject_code = r.activity_subject_code
          AND (r.validity_start_date <= TRUNC (sysdate)
                 AND ( (q.product_id IN (29, 30)
                        AND ( (r.validity_end_date IS NULL
                               AND r.sme_end_date IS NULL)
                             OR ( (r.validity_end_date IS NULL
                                   OR r.validity_end_date >= TRUNC (sysdate))
                                 AND r.sme_end_date >= TRUNC (sysdate))))
                      OR (q.product_id NOT IN (29, 30)
                          AND (r.validity_end_date >= TRUNC (sysdate)
                               OR r.validity_end_date IS NULL)))
                )
      ;

      reco_activity_subject    c_activity_subject%ROWTYPE;

      CURSOR c_degree_of_risk IS
        SELECT degree_of_risk
          FROM ocq_koc_ocp_buildings bld,
               ocq_quotes q,
               koc_oc_act_subject_ref_ext asr,
               ocq_partitions p
         WHERE bld.quote_id = p_quote_id
           AND bld.partition_no = v_partition_no
           AND q.quote_id = bld.quote_id
           AND bld.activity_subject_code = asr.activity_subject_code
           AND p.quote_id = bld.quote_id
           AND p.partition_type = asr.partition_type
           AND p.partition_no = bld.partition_no
           AND q.product_id = asr.product_id
           AND asr.validity_start_date <= TRUNC (sysdate)
           AND (asr.validity_end_date > TRUNC (sysdate)
              OR asr.validity_end_date IS NULL);

      v_degree_of_risk VARCHAR2(25);

      CURSOR c_risk_survey_item IS
        SELECT alz_risk_question_id, alz_risk_answer_id, sr.discount_ratio
        FROM   alz_risk_question q,
               alz_risk_survey_question_item qi,
               alz_risk_survey_answer_item ai,
               ocq_koc_ocp_risk_survey_result sr,
               ocq_koc_ocp_risk_survey_answer ar,
               alz_risk_survey_quest_map m,
               ocq_partitions p,
               ocq_quotes q
        WHERE  q.quote_id = p_quote_id
          AND  q.contract_id = sr.contract_id
          AND  sr.partition_no = v_partition_no
          AND  sr.alz_risk_survey_id = m.survey_id
          AND  ar.survey_result_id = sr.id
          AND  m.quest_item_id = qi.id
          AND  qi.alz_risk_question_id = q.id
          AND  ai.alz_question_item_id = qi.id
          AND  ai.id = ar.answer_id
          AND  p.contract_id = sr.contract_id
          AND  p.partition_no = sr.partition_no;



      CURSOR c_note
      IS
         SELECT   NVL (a.has_new_notes, 0) has_new_notes,     -- default value
                  NVL (a.u_w_edit, 0) u_w_edit,                 --default value
                  note_type
           FROM   koc_notes_authorization a
          WHERE       ins_obj_uid = TO_CHAR (wip_contract_id)
                  AND a.sub_level2 = v_partition_no
                  AND a.sub_level1 < (SELECT   NVL (version_no, 0) + 1
                                        FROM   wip_policy_versions
                                       WHERE   contract_id = wip_contract_id);

      reco_note            c_note%ROWTYPE;

      CURSOR c_cover
      IS
         SELECT   ce.action_code,
                  NVL (ce.adjustment_sum_insured, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1)
                     adjustment_sum_insured_tl, -- default value
                  NVL (c.sum_insured_whole_cover, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1)
                     cover_amount_tl, -- default value
                  c.cover_code,
                  ccd.cover_cat_group,
                  cd.explanation,
                  (NVL (ce.final_premium, 0) + NVL (ce.final_tax, 0))
                  * NVL (ce.premium_swf_exchg, 1)
                     gross_premium_tl,
                  NVL (ce.final_premium, 0) * NVL (ce.premium_swf_exchg, 1)
                     net_premium_tl,
                  NULL swift_code,
                  ce.premium_swf_exchg,
                  c.sum_insured_whole_cover_swf bedel_dvz_cinsi,
                  ce.sum_insured_swf_exchg bedel_kur,
                  c.ftpremium_or_whole_cover_swf prim_dvz_cinsi,
                  ce.premium_swf_exchg prim_kur,
                  0 sigorta_bedeline_dahil, -- current ve prev de önemli degil ,
                  NVL (ce.full_cover, 0) tam_teminat,
                  NVL (ce.comm_rate, 0) comm_rate,
                  NVL (ce.tariff_comm_rate, 0) tariff_comm_rate,
                  NVL (ce.clm_limit_per_year, 0)
                  * NVL (ce.sum_insured_swf_exchg, 1) clm_limit_per_year
           FROM   ocq_policy_covers c,
                  ocq_koc_ocp_policy_covers_ext ce,
                  koc_v_cover cd,
                  koc_oc_cover_definitions ccd,
                  ocq_koc_ocp_pol_versions_ext ve
          WHERE       c.quote_id = p_quote_id
                  AND c.partition_no = v_partition_no
                  AND c.quote_id = ce.quote_id
                  AND c.partition_no = ce.partition_no
                  AND c.cover_no = ce.cover_no
                  AND c.cover_code = ce.cover_code
                  AND c.quote_id = ve.quote_id
                  AND c.cover_code = cd.cover_code
                  AND c.cover_code = ccd.cover_code
                  AND ccd.validity_start_date <= ve.signature_date
                  AND (ccd.validity_end_date IS NULL
                       OR ccd.validity_end_date > ve.signature_date);

      reco_cover           c_cover%ROWTYPE;

      CURSOR c_partners_partition (
         v_role_type VARCHAR2
      )
      IS                                                                    --
         SELECT   DECODE (
                  (select count(*) from koc_cp_blacklist_entries where part_id in (
                    select part_id from koc_cp_partners_ext
                        where identity_no in (pe.identity_no) or tax_number in (pe.tax_number))
                            AND from_date <= ve.signature_date
                            AND (TO_DATE IS NULL
                                 OR TO_DATE >= ve.signature_date)),
                  0,
                  '0',
                  '1'
                 )
                  blacklist,
                 DECODE (
                    is_sisbis(ve.contract_id, pe.identity_no, pe.tax_number, il.role_type),
                    0,
                    '0',
                    '1'
                 )
                 sisbis,
                  cp.date_of_birth,
                  DECODE (cp.partner_type, 'P', cp.first_name, cp.NAME)
                     first_name,
                  pe.identity_no,
                  cp.marital_status,
                  cp.nationality,
                  cp.part_id,
                  cp.partner_type,
                  --
                  DECODE (
                     (SELECT   1
                        FROM   koc_risk_partners
                       WHERE   (tax_number = pe.tax_number
                                OR identity_no = pe.identity_no)
                               AND entry_date <= ve.signature_date
                               AND (cancel_date IS NULL
                                    OR cancel_date >= ve.signature_date)
                               AND ROWNUM < 2),
                     1,
                     '1',
                     '0'
                  )
                     risky,
                  --
                  il.role_type,
                  cp.surname,
                  pe.tax_number,
                  ip.action_code,
                  get_eh_score(ve.contract_id, pe.tax_number) eh_score,
                  is_previously_denied(ve.contract_id, pe.identity_no, pe.tax_number, il.role_type) previously_denied,
                  il.link_type,
                  NVL( ROUND((TO_DATE(SYSDATE, 'dd/mm/yyyy') - cp.DATE_OF_BIRTH) / 365),0) age,
                  NVL(pe.is_foreign_company, 0)  is_foreign_company
           FROM   ocq_interested_parties ip,
                  ocq_ip_links il,
                  ocq_koc_ocp_pol_versions_ext ve,
                  cp_partners cp,
                  koc_cp_partners_ext pe
          WHERE       ip.quote_id = p_quote_id
                  AND ip.quote_id = il.quote_id
                  AND il.partition_no = v_partition_no
                  AND ip.ip_no = il.ip_no
                  AND ip.action_code <> 'D'
                  AND ip.partner_id = cp.part_id
                  AND cp.part_id = pe.part_id
                  AND ip.quote_id = ve.quote_id
                  AND il.role_type = v_role_type;                       --'PH'

 -------------------------------------------------------------------------------------

    CURSOR c_fiziksel_bolge
    IS             -- Fiziksel Bölge Bilgisi eklendi.
    SELECT 1
      FROM koc_dmt_region_code_ref r, koc_dmt_v_current_agents d, ocq_policy_bases o
     WHERE     o.QUOTE_ID = p_quote_id
           AND d.AGEN_INT_ID = o.AGENT_ROLE
           AND r.REGION_CODE= d.region_code
           AND r.PHY_REGION_CODE <> '55';

    v_fiziksel_bolge NUMBER(1);


   CURSOR c_branch
   IS
      SELECT b.branch_ext_ref
        FROM OCQ_KOC_OCP_POL_VERSIONS_EXT a,OCQ_KOC_OCP_POL_CONTRACTS_EXT b
       WHERE    a.QUOTE_ID = p_quote_id
            AND b.QUOTE_ID = a.QUOTE_ID;

    v_branch_xref varchar2(10);
    v_is_payment_rate_changed number;
 --------------------------------------------------------------------------------------

   BEGIN
      OPEN c_policy;

      FETCH c_policy INTO reco_policy;

      IF c_policy%NOTFOUND
      THEN
         CLOSE c_policy;

         p_ocp_prv_policy := NULL;
         RETURN;
      END IF;

      CLOSE c_policy;

      OPEN c_agent;
      FETCH c_agent INTO reco_agent;
      CLOSE c_agent;


      OPEN c_fiziksel_bolge;
       FETCH c_fiziksel_bolge INTO v_fiziksel_bolge;

        IF c_fiziksel_bolge%NOTFOUND
        THEN
          v_fiziksel_bolge :=0;
        END IF;
        CLOSE c_fiziksel_bolge;

      OPEN c_branch;
      FETCH c_branch INTO v_branch_xref;
      CLOSE c_branch;

       --banka YKB
      if reco_agent.code='10503' Then
          reco_policy.contract_type :=0;
      end if;

      v_is_payment_rate_changed := 0;
      p_ocp_prv_policy :=
         CAREAR_BRE_policy (reco_policy.contract_id,
                      reco_policy.version_no,
                      reco_policy.policy_type,
                      reco_policy.term_start_date,
                      reco_policy.signature_date,
                      reco_policy.term_end_date,
                      reco_policy.product_id,
                      reco_policy.business_start_date,
                      reco_policy.group_code,
                      NULL,                                   -- CAREAR_BRE_agent
                      NULL,                                    -- CAREAR_BRE_user
                      reco_policy.endorsement_no,
                      reco_policy.contract_type,
                      reco_policy.reference_contract_id,
                      reco_policy.oip_policy_ref,
                      reco_policy.gross_premium_tl,
                      reco_policy.usd_exchg_rate,
                      reco_policy.eur_exchg_rate,
                      reco_policy.net_premium_tl,
                      reco_policy.is_renewal,
                      reco_policy.old_contract_id,
                      reco_policy.pol_signature_date,
                      reco_policy.is_industrial,
                      NULL,
                      NULL,
                      NULL,
                      reco_policy.is_date_fixed,
                      reco_policy.aff_payment_type,
                      reco_policy.bank_sales_channel,
                      reco_policy.fronting_agent_comm_rate,
                      v_branch_xref,
                      v_fiziksel_bolge,
                      reco_policy.endors_reason_code,
                      reco_policy.adj_gross_premium_tl,
                      reco_policy.bsmv_tax,
                      reco_policy.downpayment_rate,    --peþinat
                      0, --reco_policy.sum_imsured_total,   --
                      reco_policy.direkt_policy_type,       --direk endirek
                      reco_policy.is_reinsured,
                      v_is_payment_rate_changed
                     );



     IF nvl(reco_agent.int_id,0) = 0 THEN
      NULL;
   ELSE
         p_ocp_prv_policy.AGENT :=
            CAREAR_BRE_agent (reco_agent.int_id,
                           reco_agent.code,
                           reco_agent.sales_channel,
                           reco_agent.main_group,
                           reco_agent.sub_group,
                           reco_agent.category_type,
                           reco_agent.region_code);
      END IF;

      ---<--- TYH-66927 IDM Entegrasyonu -----
      -- user / role
      /*OPEN c_user;

      FETCH c_user INTO reco_user;

      IF c_user%NOTFOUND
      THEN
         NULL;
      ELSE
         p_ocp_prv_policy.carear_user :=
            CAREAR_BRE_user (reco_user.username,
                          reco_user.first_name,
                          reco_user.surname,
                          NULL,
                          reco_user.user_type,
                          reco_user.user_level);
         v_count := 0;
         p_ocp_prv_policy.carear_user.role_code := CAREAR_BRE_role_table ();

         OPEN c_role (reco_user.username);

         LOOP
            FETCH c_role INTO reco_role;

            EXIT WHEN c_role%NOTFOUND;
            v_count := v_count + 1;
            p_ocp_prv_policy.carear_user.role_code.EXTEND;
            p_ocp_prv_policy.carear_user.role_code (v_count) :=
               CAREAR_BRE_role (reco_role.role_code);
         END LOOP;

         CLOSE c_role;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.carear_user.role_code := NULL;
         END IF;
      END IF;

      CLOSE c_user;*/
      vv_username := NULL;
      vv_first_name := NULL;
      vv_surname := NULL;
      vv_user_type := NULL;


      open c_quote_user;
      fetch c_quote_user into vv_username;
      close c_quote_user;

      alz_base_function_utils.user_type(vv_username, vv_first_name, vv_surname, vv_user_type);

      IF vv_first_name IS NOT NULL THEN
         vv_user_level := get_user_level(vv_username); -- TYH-66927 IDM Entegrasyonu
         p_ocp_prv_policy.carear_user :=
            CAREAR_BRE_user (vv_username,
                          vv_first_name,
                          vv_surname,
                          NULL,
                          vv_user_type,
                          vv_user_level);
         v_count := 0;
         p_ocp_prv_policy.carear_user.role_code := CAREAR_BRE_role_table ();

         OPEN c_role (vv_username);

         LOOP
            FETCH c_role INTO reco_role;

            EXIT WHEN c_role%NOTFOUND;
            v_count := v_count + 1;
            p_ocp_prv_policy.carear_user.role_code.EXTEND;
            p_ocp_prv_policy.carear_user.role_code (v_count) :=
               CAREAR_BRE_role (reco_role.role_code);
         END LOOP;

         CLOSE c_role;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.carear_user.role_code := NULL;
         END IF;
      END IF;
      --->------------------------------------

      --
      v_count := 0;
      p_ocp_prv_policy.policy_holders := CAREAR_BRE_partner_table ();

      OPEN c_partners_ph;

      LOOP
         FETCH c_partners_ph INTO reco_partners;

         EXIT WHEN c_partners_ph%NOTFOUND;
         v_count := v_count + 1;
         p_ocp_prv_policy.policy_holders.EXTEND;
         p_ocp_prv_policy.policy_holders (v_count) :=
            CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
      END LOOP;

      CLOSE c_partners_ph;



      IF v_count = 0
      THEN
         p_ocp_prv_policy.policy_holders := NULL;
      END IF;

      OPEN find_signature_date;
      FETCH find_signature_date INTO v_signature_date;
      CLOSE find_signature_date;

      v_count := 0;
      p_ocp_prv_policy.policy_partitions := CAREAR_BRE_partition_table ();

      OPEN c_partitions;

      LOOP
         FETCH c_partitions INTO reco_partitions;

         EXIT WHEN c_partitions%NOTFOUND;
         v_partition_no := reco_partitions.partition_no;
         v_count := v_count + 1;
         p_ocp_prv_policy.policy_partitions.EXTEND;
         p_ocp_prv_policy.policy_partitions (v_count) :=
            CAREAR_BRE_partition (reco_partitions.action_code,
                            reco_partitions.partition_no,
                            reco_partitions.partition_type,
                            reco_partitions.sumins_whole_cover_by_risk_tl,
                            reco_partitions.net_premium_by_partition_tl,
                            reco_partitions.gross_premium_by_partition_tl,
                            reco_partitions.swift_code,
                            reco_partitions.def_sum_insured_swf_exchg,
                            NULL,                                    -- covers
                            NULL,                                     -- notes
                            NULL,                              -- coefficients
                            NULL,                              -- lcr_partners
                            NULL,                           --insured_partners
                            NULL,                           -- address
                            NULL,                                   --clauses
                            NULL,                          -- activity_subject
                            NULL,                           -- quote_same_quarter
                            reco_partitions.allianz_program_pol ,
                            reco_partitions.allianz_fronting_pol ,
                            reco_partitions.job_type );

         OPEN adress_coordinates (v_partition_no);
         FETCH adress_coordinates INTO coordinates;
         IF adress_coordinates%NOTFOUND THEN
             coordinates := NULL;
         END IF;
         CLOSE adress_coordinates;

         --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri aliniyor
         IF coordinates.xcoor IS NOT NULL AND coordinates.ycoor IS NOT NULL
         THEN
            v_coordinate_found := 1;

            OPEN earthquake_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH earthquake_dates INTO earthquakedates;
            IF earthquake_dates%NOTFOUND THEN
                earthquakedates := NULL;
            END IF;
            CLOSE earthquake_dates;

            OPEN flood_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH flood_dates INTO flooddates;
            IF flood_dates%NOTFOUND THEN
                flooddates := NULL;
            END IF;
            CLOSE flood_dates;

            OPEN landslide_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH landslide_dates INTO landslidedates;
            IF landslide_dates%NOTFOUND THEN
                landslidedates := NULL;
            END IF;
            CLOSE landslide_dates;

            OPEN terror_dates (coordinates.xcoor, coordinates.ycoor);
            FETCH terror_dates INTO terrordates;
            IF (terror_dates%NOTFOUND OR terrordates.validity_end_date < sysdate)THEN
                 OPEN c_in_terror_list;
                 FETCH c_in_terror_list INTO v_in_terror_list;
                 IF c_in_terror_list%NOTFOUND THEN
                   terrordates := NULL;
                 ELSE -- Teror il/ilce listesindeyse start/end date'i otorizasyona dusecek sekilde set et
                   terrordates.validity_start_date := sysdate-2;
                   terrordates.validity_end_date := sysdate+2;
                 END IF;
                 CLOSE c_in_terror_list;
            END IF;
            CLOSE terror_dates;
         END IF;
         --deprem, sel, yer kaymasi ve teror risklerinin tarih bilgileri alindi


         IF v_coordinate_found = 1
         THEN
            OPEN c_address (earthquakedates.validity_start_date,
                            earthquakedates.validity_end_date,
                            flooddates.validity_start_date,
                            flooddates.validity_end_date,
                            landslidedates.validity_start_date,
                            landslidedates.validity_end_date,
                            terrordates.validity_start_date,
                            terrordates.validity_end_date);
         ELSE
            OPEN c_address (v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date,
                            v_signature_date - 1,
                            v_default_end_date);
         END IF;

         FETCH c_address INTO reco_address;

         IF c_address%NOTFOUND
         THEN
            NULL;
         ELSE
            p_ocp_prv_policy.policy_partitions (v_count).address :=
               CAREAR_BRE_address (
                  CAREAR_BRE_basic_address (reco_address.risk_add_id,
                                         reco_address.country_code,
                                         reco_address.city_code,
                                         reco_address.district_code,
                                         reco_address.quarter_code,
                                         reco_address.adres_txt),
                  reco_address.municipality_code,
                  reco_address.earthquake_zone_code,
                  reco_address.cresta_zone_code,
                  reco_address.earthquake_auth_start_date,
                  reco_address.earthquake_auth_end_date,
                  reco_address.flood_auth_start_date,
                  reco_address.flood_auth_end_date,
                  -- landslide mahalle bazyna kadar inebilmekte  ilçede olmayyp mahallede olabilir eklenmesi gerekir.
                  reco_address.landslide_start_date,
                  reco_address.landslide_end_date,
                  reco_address.terror_start_date,
                  reco_address.terror_end_date,
                  NULL                                                 --cumul
               );

            IF coordinates.xcoor IS NOT NULL
               AND coordinates.ycoor IS NOT NULL
            THEN
               OPEN c_cumul (coordinates.xcoor, coordinates.ycoor);

               FETCH c_cumul INTO reco_cumul;

               IF c_cumul%NOTFOUND
               THEN
                  NULL;
               ELSE
                  p_ocp_prv_policy.policy_partitions (v_count).address.cumul :=
                     CAREAR_BRE_cumul (1, 'H', NULL);
               END IF;

               CLOSE c_cumul;
            ELSE
               p_ocp_prv_policy.policy_partitions (v_count).address.cumul :=
                  CAREAR_BRE_cumul (1, 'H', NULL);
            END IF;
         END IF;

         CLOSE c_address;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).coefficients :=
            CAREAR_BRE_coef_table ();

         OPEN c_coefficient;

         LOOP
            FETCH c_coefficient INTO reco_coefficient;

            EXIT WHEN c_coefficient%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).coefficients.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).coefficients (v_count2) :=
               CAREAR_BRE_coefficient (reco_coefficient.discount_saving_code, -- coefficientCode
                                    reco_coefficient.cotype,
                                    reco_coefficient.action_code,
                                    reco_coefficient.rate,
                                    reco_coefficient.exemption_value,
                                    reco_coefficient.exemption_type,
                                    reco_coefficient.is_rate_enter_manually,
                                    reco_coefficient.min_exemp_sum_ins,
                                    reco_coefficient.min_exemp_sum_ins_swf,
                                    reco_coefficient.exemp_amount_swf,
                                    reco_coefficient.tariff_disc_rate,
                                    reco_coefficient.exe_amnt_or_rate,
                                    reco_coefficient.document_group_id);
         END LOOP;

         CLOSE c_coefficient;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).coefficients := NULL;
         END IF;



         OPEN c_activity_subject;

         FETCH c_activity_subject INTO reco_activity_subject;

         IF c_activity_subject%NOTFOUND
         THEN
            NULL;
         ELSE
            OPEN c_degree_of_risk;

            FETCH c_degree_of_risk INTO v_degree_of_risk;

            CLOSE c_degree_of_risk;

            p_ocp_prv_policy.policy_partitions (v_count).activity_subject :=
               CAREAR_BRE_activity_subject (reco_activity_subject.segment,
                                        reco_activity_subject.segment_code,
                                        reco_activity_subject.subsegment,
                                        reco_activity_subject.main_activity,
                                        NVL(v_degree_of_risk, ' '),
                                        reco_activity_subject.act_subj_exp
                                        );
         END IF;

         CLOSE c_activity_subject;



         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).notes :=
            CAREAR_BRE_note_table ();

         OPEN c_note;

         LOOP
            FETCH c_note INTO reco_note;

            EXIT WHEN c_note%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).notes.EXTEND;
            p_ocp_prv_policy.policy_partitions (v_count).notes (v_count2) :=
               CAREAR_BRE_note (reco_note.has_new_notes, reco_note.u_w_edit, reco_note.note_type);
         END LOOP;

         CLOSE c_note;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).notes := NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).covers :=
            CAREAR_BRE_cover_table ();

         OPEN c_cover;

         LOOP
            FETCH c_cover INTO reco_cover;

            EXIT WHEN c_cover%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).covers.EXTEND;
            p_ocp_prv_policy.policy_partitions (v_count).covers (v_count2) :=
               CAREAR_BRE_cover (reco_cover.cover_code,
                           reco_cover.explanation,
                           --cover_type              ,
                           reco_cover.cover_cat_group,
                           reco_cover.cover_amount_tl,
                           reco_cover.adjustment_sum_insured_tl,
                           reco_cover.net_premium_tl,
                           reco_cover.gross_premium_tl,
                           reco_cover.action_code,
                           reco_cover.bedel_dvz_cinsi,
                           reco_cover.bedel_kur,
                           reco_cover.prim_dvz_cinsi,
                           reco_cover.prim_kur,
                           reco_cover.sigorta_bedeline_dahil,
                           reco_cover.tam_teminat,
                           reco_cover.comm_rate,
                           reco_cover.tariff_comm_rate,
                           reco_cover.clm_limit_per_year
                           );
         END LOOP;

         CLOSE c_cover;

         IF v_count2 = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).covers := NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).insured_partners :=
            CAREAR_BRE_partner_table ();

         OPEN c_partners_partition ('INS');

         LOOP
            FETCH c_partners_partition INTO reco_partners;

            EXIT WHEN c_partners_partition%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).insured_partners.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).insured_partners (v_count2) :=
               CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
         END LOOP;

         CLOSE c_partners_partition;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).insured_partners :=
               NULL;
         END IF;

         v_count2 := 0;
         p_ocp_prv_policy.policy_partitions (v_count).lcr_partners :=
            CAREAR_BRE_partner_table ();

         OPEN c_partners_partition ('LCR');

         LOOP
            FETCH c_partners_partition INTO reco_partners;

            EXIT WHEN c_partners_partition%NOTFOUND;
            v_count2 := v_count2 + 1;
            p_ocp_prv_policy.policy_partitions (v_count).lcr_partners.EXTEND;
            p_ocp_prv_policy.policy_partitions (
               v_count
            ).lcr_partners (v_count2) :=
               CAREAR_BRE_partner (reco_partners.part_id,
                          reco_partners.first_name,
                          reco_partners.surname,
                          reco_partners.date_of_birth,
                          reco_partners.nationality,
                          reco_partners.marital_status,
                          reco_partners.identity_no,
                          reco_partners.tax_number,
                          reco_partners.role_type,
                          reco_partners.risky,
                          reco_partners.blacklist,
                          reco_partners.partner_type,
                          reco_partners.action_code,
                          0,
                          0,
                          0,
                          NULL,
                          reco_partners.link_type,
                          reco_partners.age,
                          reco_partners.sisbis,
                          reco_partners.eh_score,
                          reco_partners.previously_denied,
                          reco_partners.is_foreign_company,
                          0,
                          0);
         END LOOP;

         CLOSE c_partners_partition;

         IF v_count = 0
         THEN
            p_ocp_prv_policy.policy_partitions (v_count).lcr_partners := NULL;
         END IF;
      END LOOP;

      CLOSE c_partitions;

      IF v_count = 0
      THEN
         p_ocp_prv_policy.policy_partitions := NULL;
      END IF;

      -- önceki poliçe ve hasarlar , ocp ve prev için kullanilmiyor
      p_ocp_prv_policy.prev_carear_claims := NULL;
      RETURN;
   EXCEPTION
      WHEN OTHERS
      THEN
         alz_web_process_utils.process_result (
            0,
            9,
            -1,
            'INVALID_DATA',
            'OCP/PREV HATA',
            'OCP/PREV HATA',
            NULL,
            NULL,
            'alz_carear_bre_utils , p_OCP_PRV',
            NULL,
            p_process_results
         );
   END p_ocq_prv;


   PROCEDURE p_carear_policy_info (
      p_wip_contract_id       NUMBER,
      p_wip_policy        OUT CAREAR_BRE_policy,
      p_ocp_policy        OUT CAREAR_BRE_policy,
      p_prv_policy        OUT CAREAR_BRE_policy,
      p_process_results   OUT customer.process_result_table
   )
   IS
      v_contract_id   NUMBER (10);

      CURSOR c_quote
      IS
         SELECT   Q.QUOTE_ID
           FROM   wip_koc_ocp_pol_contracts_ext ce,
                  wip_policy_versions v,
                  ocq_quotes q
          WHERE       CE.CONTRACT_ID = V.CONTRACT_ID
                  AND CE.PREV_QUOTE_REF = Q.QUOTE_REFERENCE
                  AND CE.CONTRACT_ID = p_wip_contract_id;

      CURSOR c_policy
      IS
         SELECT   POLICY_REF
           FROM   ocp_policy_bases
          WHERE   CONTRACT_ID = p_wip_contract_id;

      v_quote_id      OCQ_QUOTES.QUOTE_ID%TYPE := NULL;
      v_policy_ref    OCP_POLICY_BASES.POLICY_REF%TYPE := NULL;
   BEGIN
      p_wip (p_wip_contract_id, p_wip_policy, p_process_results);


      IF p_process_results IS NOT NULL
      THEN
         IF p_process_results.COUNT > 0
         THEN
            RETURN;
         END IF;
      END IF;

      OPEN c_quote;
      FETCH c_quote INTO v_quote_id;
      CLOSE c_quote;

      OPEN c_policy;
      FETCH c_policy INTO v_policy_ref;
      CLOSE c_policy;

      IF v_policy_ref IS NOT NULL
      THEN
         -- OCP
         p_ocp_prv (p_wip_contract_id,
                    p_wip_contract_id,
                    p_ocp_policy,
                    p_process_results);

      ELSIF v_quote_id IS NOT NULL --to do
      THEN
         -- OCQ
         p_ocq_prv (v_quote_id,
                    p_wip_contract_id,
                    p_ocp_policy,
                    p_process_results);
      END IF;

      IF p_process_results IS NOT NULL
      THEN
         IF p_process_results.COUNT > 0
         THEN
            RETURN;
         END IF;
      END IF;

      v_contract_id := find_old_contract_id (p_wip_contract_id); --yks'nin poliçe bilgileri üzerinden iþlem yapmadýðýmýzdan bu fonksiyonu bri_... ile replace etmedik
      -- PREV
      p_ocp_prv (v_contract_id,
                 p_wip_contract_id,
                 p_prv_policy,
                 p_process_results);
      RETURN;
   END p_carear_policy_info;


   FUNCTION write_to_authorization_table (v_contract_id     IN NUMBER,
                                          v_version_no      IN NUMBER,
                                          v_partition_no    IN NUMBER,
                                          v_auth_no         IN NUMBER,
                                          v_username        IN VARCHAR2,
                                          v_partaj          IN VARCHAR2,
                                          v_description     IN VARCHAR2,
                                          v_limit_auth_no   IN NUMBER)
      RETURN VARCHAR2
   IS

     CURSOR c_if_exist
        IS
         SELECT   DISTINCT auth_grant_uname, auth_grant_date
           FROM   koc_pol_authorization a,
                  dmt_agents d
          WHERE       a.contract_id = v_contract_id
                  AND a.version_no = NVL (v_version_no, 0) + 1
                  AND a.auth_no = v_auth_no
                  AND a.partition_no = v_partition_no
                  AND a.agent_role = d.int_id
                  AND d.reference_code = v_partaj
                  AND NVL (a.limit_auth_no, 0) = NVL (v_limit_auth_no, 0);

      CURSOR c_max_auth_refno
      IS
         SELECT   koc_pol_authorization_seq.NEXTVAL FROM DUAL;

      /*NVL(MAX(auth_ref_no),0)+1
       FROM koc_pol_authorization;*/
      CURSOR POLICY
      IS
         SELECT   NVL (a.is_industrial, 0) is_industrial,
                  DECODE (b.move_code, 'FMBQ', 'T', 'FSQ', 'T', 'P')
                     contract_type,
                  c.agent_role,
                  a.region_code
           FROM   wip_koc_ocp_pol_versions_ext a,
                  wip_policy_versions b,
                  wip_policy_bases c
          WHERE       a.contract_id = v_contract_id
                  AND a.contract_id = b.contract_id
                  AND a.contract_id = c.contract_id;

      max_auth_refno       NUMBER;
      cv_auth_grant_name   VARCHAR2 (30);
      cv_auth_grant_date   DATE;
      v_found_factor              BOOLEAN;
      v_found                     BOOLEAN;
      v_policy             POLICY%ROWTYPE;
      v_errm               VARCHAR2 (2000);
   BEGIN


      OPEN c_if_exist;

      FETCH c_if_exist
      INTO   cv_auth_grant_name, cv_auth_grant_date;

      IF c_if_exist%FOUND
      THEN
         v_found_factor := TRUE;
      ELSE
         v_found_factor := FALSE;
         cv_auth_grant_name := NULL;
      END IF;

      CLOSE c_if_exist;

      IF cv_auth_grant_name IS NULL
         OR SUBSTR (cv_auth_grant_name, 1, 7) = 'NOGRANT'
      THEN
         IF v_found_factor
         THEN
            UPDATE   koc_pol_authorization
               SET   auth_entry_date = SYSDATE,
                     auth_entry_uname = v_username,
                     auth_grant_date = NULL,
                     auth_grant_uname = NULL
             WHERE       contract_id = v_contract_id
                     AND version_no = NVL (v_version_no, 0) + 1
                     AND auth_no = v_auth_no
                     AND partition_no = v_partition_no
                     AND NVL (limit_auth_no, 0) = NVL (v_limit_auth_no, 0);
         ELSE
            OPEN c_max_auth_refno;

            FETCH c_max_auth_refno INTO max_auth_refno;

            CLOSE c_max_auth_refno;

            OPEN POLICY;

            FETCH POLICY INTO v_policy;

            CLOSE POLICY;

            INSERT INTO koc_pol_authorization (auth_ref_no,
                                               contract_id,
                                               version_no,
                                               partition_no,
                                               auth_no,
                                               auth_entry_date,
                                               auth_entry_uname,
                                               description,
                                               is_industrial,
                                               auth_con_type,
                                               agent_role,
                                               region_code,
                                               limit_auth_no)
              VALUES   (max_auth_refno,
                        v_contract_id,
                        NVL (v_version_no, 0) + 1,
                        v_partition_no,
                        v_auth_no,
                        SYSDATE,
                        v_username,
                        v_description,
                        v_policy.is_industrial,
                        v_policy.contract_type,
                        v_policy.agent_role,
                        v_policy.region_code,
                        v_limit_auth_no);
         END IF;

         RETURN ('1');
      ELSE
         RETURN ('0');
      END IF;
   END;

   PROCEDURE p_write_authorization (
      p_contract_id           NUMBER,
      p_policy                CAREAR_BRE_RULE_TABLE,
      p_partititons           CAREAR_BRE_PART_RULE_TABLE,
      p_process_results   OUT PROCESS_RESULT_TABLE
   )
   IS
      v_version_no     NUMBER (5);
      v_partition_no   NUMBER (5);
      v_username       VARCHAR2 (30);
      v_partaj         VARCHAR2 (15);
      v_description    VARCHAR2 (500);
      v_result         VARCHAR2 (255);

      CURSOR c_partitions
      IS
         SELECT   partition_no
           FROM   wip_partitions
          WHERE   contract_id = p_contract_id;

      auth_write_err EXCEPTION;

      CURSOR c_partaj (
         p_username VARCHAR2
      )
      IS
         SELECT   reference_code
           FROM   dmt_agents a,
                  koc_cp_partners_ext b,
                  koc_v_sec_system_users c
          WHERE       a.int_id = b.agen_int_id
                  AND b.part_id = c.customer_partner_id
                  AND c.oracle_username = p_username;

      CURSOR c_policy_partaj
      IS
        SELECT d.REFERENCE_CODE
          FROM wip_policy_bases w,dmt_agents d
         WHERE w.contract_id = p_contract_id
                AND w.AGENT_ROLE = d.INT_ID;

   BEGIN
      SELECT   version_no
        INTO   v_version_no
        FROM   wip_policy_versions
       WHERE   contract_id = p_contract_id;

      SELECT   username
        INTO   v_username
        FROM   wip_policy_versions
       WHERE   contract_id = p_contract_id;

        OPEN c_policy_partaj;

        FETCH c_policy_partaj INTO v_partaj;

        CLOSE c_policy_partaj;

      DELETE   koc_pol_authorization
       WHERE       contract_id = p_contract_id
               AND version_no = NVL (v_version_no, 0) + 1
               AND auth_grant_date IS NULL;

      --AND auth_no=v_auth_no and partition_no=v_partition_no
      IF p_policy IS NULL
      THEN
         NULL;
      ELSE
         IF p_policy.COUNT > 0
         THEN
            FOR i IN p_policy.FIRST .. p_policy.LAST
            LOOP
               SELECT   auth_definition
                 INTO   v_description
                 FROM   alz_authorization_def
                WHERE   auth_no = p_policy (i).auth_no;

               OPEN c_partitions;

               LOOP
                  FETCH c_partitions INTO v_partition_no;

                  EXIT WHEN c_partitions%NOTFOUND;
                  v_result :=                                --Koc_Auth_Utils.
                     write_to_authorization_table (p_contract_id,
                                                   v_version_no,
                                                   v_partition_no,
                                                   p_policy (i).auth_no,
                                                   v_username,
                                                   v_partaj,
                                                   v_description,
                                                   NULL);

                  IF v_result NOT IN ('0', '1')
                  THEN
                     RAISE auth_write_err;
                  END IF;
               END LOOP;

               CLOSE c_partitions;
            END LOOP;
         END IF;
      END IF;

      IF p_partititons IS NULL
      THEN
         NULL;
      ELSE
         IF p_partititons.COUNT > 0
         THEN
            FOR i IN p_partititons.FIRST .. p_partititons.LAST
            LOOP
               IF p_partititons (i).carear_rules IS NULL
               THEN
                  NULL;
               ELSE
                  IF p_partititons (i).carear_rules.COUNT > 0
                  THEN
                     FOR j IN p_partititons (i).carear_rules.FIRST .. p_partititons(i).carear_rules.LAST
                     LOOP
                        SELECT   auth_definition
                          INTO   v_description
                          FROM   alz_authorization_def
                         WHERE   auth_no =
                                    p_partititons (i).carear_rules (j).auth_no;

                        --commit;
                        v_result :=                          --Koc_Auth_Utils.
                           write_to_authorization_table (
                              p_contract_id,
                              v_version_no,
                              p_partititons (i).partition_no,
                              p_partititons (i).carear_rules (j).auth_no,
                              v_username,
                              v_partaj,
                              v_description,
                              NULL
                           );

                        IF v_result NOT IN ('1', '0')
                        THEN
                           RAISE auth_write_err;
                        END IF;
                     END LOOP;
                  END IF;
               END IF;
            END LOOP;
         END IF;
      END IF;

      RETURN;
   /* exception when auth_write_err then

   alz_web_process_utils.process_result ( 0,9,-1 ,'INVALID_DATA' , 'Authorization kaydedilemedi' ,
   'Authorization kaydedilemedi' , null,null,'alz_carear_bre_utils , p_write_authorization',null,p_process_results ) ;

   WHEN others THEN
   alz_web_process_utils.process_result ( 0,9,-1 ,'INVALID_DATA' , 'p_write_authorization hata : '||sqlerrm ,
   'p_write_authorization hata : '||sqlerrm  , null,null,'alz_carear_bre_utils , p_write_authorization',null,p_process_results ) ;
   */
   EXCEPTION
      WHEN OTHERS
      THEN
         alz_web_process_utils.process_result (
            0,
            9,
            -1,
            'ORACLE_EXCEPTION',
            SUBSTR (
                  SQLCODE
               || ' - '
               || SQLERRM
               || ' - '
               || DBMS_UTILITY.format_error_backtrace,
               1,
               1000
            ),
            NULL,
            NULL,
            p_contract_id,
            'alz_carear_bre_utils.p_write_authorization',
            v_username,
            p_process_results
         );
   END p_write_authorization;


    FUNCTION is_sisbis (p_contract_id NUMBER,
                       p_identity_no VARCHAR2,
                       p_tax_no      VARCHAR2,
                       p_role_type   VARCHAR2) RETURN NUMBER
    IS
        ocp_partner_exist NUMBER;
        wip_partner_exist NUMBER;
        v_sisbis          NUMBER;
        v_authStatus      VARCHAR(10);

        CURSOR curWipTCKvkn is
           SELECT count(*)
              FROM WIP_IP_LINKS IPL, WIP_INTERESTED_PARTIES IP,
                    KOC_CP_PARTNERS_EXT  PE
              WHERE IP.CONTRACT_ID = IPL.CONTRACT_ID
                AND IP.IP_NO = IPL.IP_NO
                AND IPL.CONTRACT_ID = p_contract_id
                AND IPL.ROLE_TYPE = p_role_type
                AND PE.PART_ID = IP.PARTNER_ID
                AND (PE.TAX_NUMBER = p_tax_no OR PE.IDENTITY_NO = p_identity_no);

        CURSOR curOcpTCKvkn is
            SELECT count(*)
               FROM OCP_IP_LINKS IPL, OCP_INTERESTED_PARTIES IP,
                    KOC_CP_PARTNERS_EXT  PE
              WHERE IP.CONTRACT_ID = IPL.CONTRACT_ID
                AND IP.IP_NO = IPL.IP_NO
                AND IPL.CONTRACT_ID = p_contract_id
                AND IPL.ROLE_TYPE = p_role_type
                AND PE.PART_ID = IP.PARTNER_ID
                AND (PE.TAX_NUMBER = p_tax_no OR PE.IDENTITY_NO = p_identity_no);

        CURSOR curSisbis is
          SELECT count(*) FROM alz_ins_corruption
          WHERE SUSPECT_IDENTITY = p_identity_no or SUSPECT_TAX_NUMBER = p_tax_no;

        CURSOR curAuthStatus is
            SELECT AUTH_STATUS
            FROM ALZ_BPM_AUTH_DETAIL
            WHERE CONTRACT_ID = p_contract_id
                AND AUTH_NO = 10116
            ORDER BY VERSION_NO DESC, ORDER_NO DESC;
    BEGIN
        OPEN  curSisbis;
        FETCH curSisbis INTO v_sisbis;
        CLOSE curSisbis;

        IF  v_sisbis <> 0 THEN

            OPEN  curWipTCKvkn;
            FETCH curWipTCKvkn INTO wip_partner_exist;
            CLOSE curWipTCKvkn;

            OPEN  curOcpTCKvkn;
            FETCH curOcpTCKvkn INTO ocp_partner_exist;
            CLOSE curOcpTCKvkn;

            IF wip_partner_exist <> ocp_partner_exist THEN -- Aský sigortalýsý deðiþmiþ ve sisbis'te kaydý varsa
                return 1;

            ELSE --Aský sigortalýsý deðiþmemiþ ama sisbis'te kaydý var, daha önce onay almýþ mý diye bakýyoruz

                OPEN curAuthStatus;
                FETCH curAuthStatus INTO v_authstatus;
                CLOSE curAuthStatus;

                IF v_authStatus <> 'GRANT' THEN
                    return 1;
                END IF;
            END IF;

        END IF;

        return 0;

    END;


    FUNCTION is_previously_denied (p_contract_id NUMBER,
                                   p_identity_no VARCHAR2,
                                   p_tax_number  VARCHAR2,
                                   p_role_type   VARCHAR2) RETURN NUMBER
    IS

        v_has_nogrant NUMBER := 0;

        CURSOR c_has_nogrant
        IS
           SELECT 1
             FROM koc_cp_partners_ext pe,
                  wip_interested_parties ip,
                  wip_ip_links il,
                  alz_bpm_auth_detail ad
            WHERE ad.auth_status = 'NOGRANT'
                  AND ad.contract_id = ip.contract_id
                  AND pe.part_id = ip.partner_id
                  AND ip.ip_no = il.ip_no
                  AND ip.contract_id = il.contract_id
                  AND il.role_type = p_role_type
                  AND (pe.identity_no = p_identity_no
                          or pe.tax_number = p_tax_number)
                  AND ROWNUM = 1;

    BEGIN

        OPEN  c_has_nogrant;
        FETCH c_has_nogrant INTO v_has_nogrant;
        CLOSE c_has_nogrant;

        RETURN v_has_nogrant;

    END;


    FUNCTION get_eh_score (p_contract_id NUMBER,
                           p_tax_number VARCHAR2) RETURN NUMBER
    IS
        v_eh_score NUMBER := 0;

        CURSOR c_eh_score IS
        SELECT * FROM (
            SELECT eh_score
              FROM eh_score
             WHERE contract_id = p_contract_id
                   AND vkn = p_tax_number
                   ORDER BY eh_date desc)
        WHERE ROWNUM < 2;

    BEGIN

        OPEN  c_eh_score;
        FETCH c_eh_score INTO v_eh_score;
        CLOSE c_eh_score;

        RETURN v_eh_score;

    END;


    FUNCTION get_user_level (
        p_username  IN   VARCHAR2
    ) RETURN NUMBER
    IS

    v_user_type NUMBER;
    v_user_level NUMBER;

    CURSOR c_user_level (v_username VARCHAR2)
    IS
        SELECT NVL(user_level, 0) user_level
          FROM alz_bpm_user_escalation
         WHERE username = v_username
               AND ROWNUM = 1;

    CURSOR c_user_type (v_username VARCHAR2)
    IS
        SELECT user_type
          FROM ALZ_BPM_UNDERWRITER
         WHERE user_name = v_username;

    BEGIN
        OPEN c_user_level (p_username);
        FETCH c_user_level INTO v_user_level;
        CLOSE c_user_level;


        OPEN c_user_type (p_username);
        FETCH c_user_type INTO v_user_type;

        IF v_user_level = 1
        THEN
            IF v_user_type = 1
            THEN
               v_user_level := 0;
            END IF;

            IF c_user_type%NOTFOUND
            THEN
               v_user_level := 0;
            END IF;
        END IF;

        IF v_user_level IS NULL
        THEN
            v_user_level := 0;
        END IF;

        CLOSE c_user_type;

        RETURN v_user_level;
    END;


    FUNCTION is_payment_rate_changed(p_contract_id NUMBER , p_product_id NUMBER)RETURN NUMBER
    IS

    v_payment_rate NUMBER(10,3);  -- defautl olarak gelen taksit ve peþinat yüzdeleri
    v_payment_no NUMBER(10);  --peþinat + taksit sayýsý
    v_result NUMBER(1);
    v_last_payment_no number(10); --zeyiller dahil en son yapýlan taksit sayýsý
    v_advance_pay_rate number;

    CURSOR c_payment_table ( c_payment_rate NUMBER) IS
    SELECT round(a.rate,3 ) rate , a.payment_order_no
      FROM wip_koc_ocp_payment_plan a
     WHERE a.contract_id = p_contract_id
           AND version_no is null ;

     BEGIN
     SELECT (1 - min_advance_pay_rate)
       INTO v_advance_pay_rate
       FROM koc_oc_product_versions a
      WHERE a.product_id = p_product_id ;


     SELECT Count(payment_order_no)
       INTO v_payment_no
       FROM wip_koc_ocp_payment_plan a     --peþinat + kaç taksit yapýldýðýnýn sayýsýný verir
      WHERE contract_id = p_contract_id
            AND version_no is null;


     SELECT MAX(payment_order_no)
       INTO v_last_payment_no
       FROM wip_koc_ocp_payment_plan a      --payment_order_nolar 1 den baþlayýp artarak devam eder. En sonuncu taksit noyu alýrýz.
      WHERE contract_id = p_contract_id
            AND version_no is null;

         IF (v_payment_no = 1) THEN  -- payment_no = 1 demek peþin ödeme demektir
              v_payment_rate := 1 ;
         ELSIF(v_payment_no = 0 ) THEN -- Begin den sonraki ilk selecte deðer gelmesse v_payment_no ya 0 atanýr. Buda ödeme planý ile ilgili deðiþiklik olmadýðýnýn göstergesidir
           v_result := 0 ;    -- o yüzden v_resulta 0 deriz.
         ELSE
          v_payment_rate := v_advance_pay_rate /(v_payment_no -1 ) ; --v_payment_no (PEÞINAT + TAKSIT SAYISI)dýr. -1 DEME sebebimiz sadece taksit sayýsýný bulmakdýr.
        END IF;


         FOR MN IN c_payment_table(v_payment_rate) LOOP  --v_payment_rate =  default olarak gelen ödeme planý yüzdesindeki taksit oranlarýdýr.

            IF(MN.payment_order_no = (v_last_payment_no - v_payment_no + 1) and mn.rate = 1 ) THEN
             v_result := 0;
             EXIT;
            END IF;

             IF(MN.payment_order_no = (v_last_payment_no - v_payment_no + 1) and mn.rate <>0.25 ) THEN
             v_result := 1;
             EXIT;
             ELSIF(MN.payment_order_no <> (v_last_payment_no - v_payment_no + 1) AND MN.rate <> round(v_payment_rate , 3) ) THEN
             v_result := 1;
             EXIT;
               ELSE
             V_RESULT := 0;
             END IF;

            END LOOP;


           return v_result ;


         EXCEPTION
              WHEN OTHERS
              THEN
           return 0 ;
         END;

    END;
/

