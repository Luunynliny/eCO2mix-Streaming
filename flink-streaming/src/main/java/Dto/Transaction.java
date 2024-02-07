package Dto;

import lombok.Data;

@Data
public class Transaction {
    private String date_heure;
    private Integer consommation;
    private Integer prevision_j1;
    private Integer prevision_j;
    private Integer fioul;
    private Integer charbon;
    private Integer gaz;
    private Integer nucleaire;
    private Integer eolien;
    private Integer eolien_terrestre;
    private Integer eolien_offshore;
    private Integer solaire;
    private Integer hydraulique;
    private Integer pompage;
    private Integer bioenergies;
    private Integer ech_physiques;
    private Integer taux_co2;
    private Integer ech_comm_angleterre;
    private Integer ech_comm_espagne;
    private Integer ech_comm_italie;
    private Integer ech_comm_suisse;
    private Integer ech_comm_allemagne_belgique;
    private Integer fioul_tac;
    private Integer fioul_cogen;
    private Integer fioul_autres;
    private Integer gaz_tac;
    private Integer gaz_cogen;
    private Integer gaz_ccg;
    private Integer gaz_autres;
    private Integer hydraulique_fil_eau_eclusee;
    private Integer hydraulique_lacs;
    private Integer hydraulique_step_turbinage;
    private Integer bioenergies_dechets;
    private Integer bioenergies_biomasse;
    private Integer bioenergies_biogaz;
    private Integer stockage_batterie;
    private Integer destockage_batterie;
}
