CREATE OR REPLACE PACKAGE CUSTOMER."ALZ_CAREAR_BRE_UTILS"
IS
   TYPE refcur IS REF CURSOR;

   TYPE number_table IS TABLE OF NUMBER
                           INDEX BY BINARY_INTEGER;

   PROCEDURE p_carear_policy_info (
      p_wip_contract_id       NUMBER,
      p_wip_policy        OUT CAREAR_BRE_POLICY,
      p_ocp_policy        OUT CAREAR_BRE_POLICY,
      p_prv_policy        OUT CAREAR_BRE_POLICY,
      p_process_results   OUT CUSTOMER.PROCESS_RESULT_TABLE
   );

   PROCEDURE p_write_authorization (
      p_contract_id           NUMBER,
      p_policy                CAREAR_BRE_RULE_TABLE,
      p_partititons           CAREAR_BRE_PART_RULE_TABLE,
      p_process_results   OUT PROCESS_RESULT_TABLE
   );

   PROCEDURE multiplefactorpolicy (p_part_id            NUMBER,
                             p_found_factor           OUT NUMBER);


   PROCEDURE multiplefactorpolicy  (p_part_id            NUMBER,
                             p_agent_role         NUMBER,
                             p_partner_type       VARCHAR2,
                             p_found_factor          OUT NUMBER);


    FUNCTION write_to_authorization_table (
      v_contract_id       IN NUMBER,
      v_version_no        IN NUMBER,
      v_partition_no      IN NUMBER,
      v_auth_no           IN NUMBER,
      v_username          IN VARCHAR2,
      v_partaj            IN VARCHAR2,
      v_description       IN VARCHAR2,
      v_limit_auth_no     IN NUMBER) RETURN VARCHAR2;

--   PROCEDURE multiplepolicy (p_identity_no       VARCHAR2,
  --                           p_vkn_no            VARCHAR2,
    --                         p_partner_type       VARCHAR2,
      --                       p_found         OUT NUMBER);


   PROCEDURE multiplepolicy (p_identity_no       VARCHAR2,
                             p_vkn_no            VARCHAR2,
                             p_agent_role        number,
                             p_partner_type       VARCHAR2,
                             p_found         OUT NUMBER);

   PROCEDURE p_ocq_prv (
      p_quote_id              NUMBER,
      wip_contract_id         NUMBER,
      p_ocp_prv_policy    OUT CAREAR_BRE_POLICY,
      p_process_results   OUT CUSTOMER.PROCESS_RESULT_TABLE
   );

   FUNCTION bre_find_old_contract_id (p_contract_id IN NUMBER) RETURN NUMBER ;

   FUNCTION is_sisbis (
      p_contract_id           NUMBER,
      p_identity_no           VARCHAR2,
      p_tax_no                VARCHAR2,
      p_role_type             VARCHAR2) RETURN NUMBER;

   FUNCTION is_previously_denied (
      p_contract_id           NUMBER,
      p_identity_no           VARCHAR2,
      p_tax_number            VARCHAR2,
      p_role_type             VARCHAR2) RETURN NUMBER;

   FUNCTION get_eh_score (
      p_contract_id           NUMBER,
      p_tax_number            VARCHAR2) RETURN NUMBER;
   FUNCTION get_user_level (
      p_username           IN   VARCHAR2
    )RETURN NUMBER;
   FUNCTION is_payment_rate_changed(
      p_contract_id           NUMBER,
      p_product_id            NUMBER)RETURN NUMBER;

END ALZ_CAREAR_BRE_UTILS;
/

