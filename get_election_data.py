#!/usr/bin/env python
# coding: utf8

from __future__ import unicode_literals

import pandas as pd
import json
from json import encoder

encoder.FLOAT_REPR = lambda o: format(o, '.2f')

def calculer_totaux(df):
    stats_index = ['departement', 'commune_code', 'tour']
    choix_index = stats_index + ['choix']

    # on vérifie que le nombre d'inscrits, votants et exprimes est le même à chaque ligne d'un même bureau
    verif_unique = df.groupby(stats_index + ['bureau']).agg({
        'inscrits': 'nunique',
        'votants': 'nunique',
        'exprimes': 'nunique',
    })
    assert (verif_unique == 1).all().all()

    stats = (
        df
            # on a vérifié que les stats étaient les mêmes pour chaque bureau, donc on déduplique en prenant
            # la première valeur
            .groupby(stats_index + ['bureau']).agg({
            'inscrits': 'first',
            'votants': 'first',
            'exprimes': 'first',
        })
            # puis on somme par commune
            .groupby(level=stats_index).sum()
            # puis on dépile le numéro de tour et on le met en premier index de colonne
            .unstack(['tour']).swaplevel(0, 1, axis=1).sortlevel(axis=1)
            # enfin, on remplace les valeurs manquantes avec des 0 (pour les élections sans second tour)
            .fillna(0, downcast='infer')
    )
    stats.columns = stats.columns.set_names(['tour', 'statistique'])

    # le fillna est utilisé pour les législatives : toutes les nuances ne sont pas présentes dans toutes
    # les circos, donc il faut remplacer les valeurs manquantes par des 0, et recaster en int
    choix = df.groupby(choix_index)['voix'].sum().unstack(['tour', 'choix']).fillna(0, downcast='infer').sortlevel(axis=1)

    # on vérifie que le nombre de suffrages exprimés est égal à la somme des votes pour chaque choix, et ce pour chaque
    # tour de l'élection
    assert (
       stats.swaplevel(0, 1, axis=1)['exprimes'] == choix.sum(axis=1, level=0)
    ).all().all()

    return stats, choix

# Codes postaux
# ATTENTION: La colonne "departement" du csv est nommee ici "nomdepartement".
# Ceci permet de maintenir la coherence des noms des index avec les autres dataframes.
allinseeinfos = pd.read_csv(
    'data/inseeinfos.csv',
    sep=';',
    skiprows=1,
    encoding="UTF-8",
    names=['insee', 'codespostaux', 'communes'],
    dtype={"insee": str, "codespostaux": str},
    usecols=[0, 1, 2]
)
allinseeinfos = allinseeinfos.reset_index()
allinseeinfos['departement'] = allinseeinfos.insee.str.slice(0,2)
allinseeinfos['commune_code'] = allinseeinfos.insee.str.slice(2)
allinseeinfos = allinseeinfos.set_index(['departement', 'commune_code'])

allinseeinfos['listecodespostaux'] = allinseeinfos.codespostaux.apply(lambda x: str(x).split('/'))

inseeinfos = allinseeinfos.ix[:, 'listecodespostaux']
# Manque:
# - L'ensemble du département "98"
# - Les ZX/ZS : la conversion vers le code Insee est ok, mais enregistrement manquant dans la base des codes insee.
# - ZA: plusieurs communes manques.
# Les 98 sont récupérés depuis une autre base, et une liste "hard codé" apporte les cas manquants
specialToInseeConversion = {
    "ZA":lambda x: x+97000, # OK
    "ZB":lambda x: x+97000, # OK
    "ZC":lambda x: x+97000, # OK
    "ZD":lambda x: x+97000, # KO
    "ZM":lambda x: x+97100,
    "ZN":lambda x: x+98000, # KO: No information about 98xxx codes
    "ZP":lambda x: x+98700, # KO: No information about 98xxx codes
    "ZS":lambda x: x+97000, # KO: Only Miquelon-Langlade, which is not referenced in insee database
    "ZX":lambda x: x+97000, # KO
    "ZW":lambda x: 'ERROR', # Wallis-et-Futuna, managed by superSpecialToPostalConversion
    "ZZ":lambda x:'00000',
}

# Ici, on convertie les cas vraiment particulier, en code postal directement.
superSpecialToPostalConversion = {
    "ZA123": "97133", # SAint-Barthélemy
    "ZA127": "97150", # Saint-Martin (pour pres_2007)
    "ZS501": "97500", # Miquelon-Langlade
    "ZS502": "97410", # Saint-Pierre
    "ZX701": "97133", # Saint-Barthélemy
    "ZX801": "97150", # Saint-Martin (pour pres/legi_2012)
    "ZW001": "98620",
}

def convertSpecialToCodePostal(specialCode):
    try:
        if specialCode in superSpecialToPostalConversion:
            return superSpecialToPostalConversion[specialCode]
        else:
            specialDep = specialCode[0:2]
            specialCommune = int(specialCode[2:])

            try:
                insee = str(specialToInseeConversion[specialDep](specialCommune))
                if insee == '00000':
                    return '00000'
                elif insee in superSpecialToPostalConversion:
                    return superSpecialToPostalConversion[insee]
                elif insee[0:2] == '98':
                    return ["NOT_FOUND"] # Replace by list of 98xxx insee codes
                else:
                    return allinseeinfos.get_value((insee[0:2], insee[2:]), "listecodespostaux")
            except KeyError:
                print "Special code '%s' has insee code '%s', but not postal code associated" % (specialCode, insee)
                return ["NOT_FOUND"]
    except KeyError:
        print "Special code '%s' cannot be handled" % specialCode
        return ["NOT_FOUND"]

def calculer_scores(stats, choix, nonistes_gauche, nonistes_droite):
    scores = 100 * choix[1].divide(stats[1]['inscrits'], axis=0)
    scores['NONISTES_DROITE'] = scores[nonistes_droite].sum(axis=1)
    scores['NONISTES_GAUCHE'] = scores[nonistes_gauche].sum(axis=1)
    scores['NONISTES'] = scores['NONISTES_DROITE'] + scores['NONISTES_GAUCHE']
    return scores


use_columns = [
    'tour', 'departement', 'commune_code', 'bureau',
    'inscrits', 'votants', 'exprimes',
    'choix', 'voix'
]


# Pour 2005

df_2005 = pd.read_csv(
    'data/2005.csv',
    sep=';',
    skiprows=20,
    encoding='cp1252',
    names=['tour', 'region', 'departement', 'arrondissement', 'circo', 'canton', 'commune_code', 'ref_inscrits',
           'commune_nom', 'bureau', 'inscrits', 'votants', 'abstentions', 'exprimes', 'choix', 'voix'],
    dtype={'departement': str, 'commune_code': str, 'bureau': str},
    usecols=use_columns
)
# attention aux espaces en trop dans la réponse
df_2005['choix'] = df_2005.choix.str.strip()

stats_2005, choix_2005 = calculer_totaux(df_2005)

# 2007 maintenant

df_2007 = pd.read_csv(
    'data/pres_2007.csv',
    sep=';',
    skiprows=17,
    encoding='cp1252',
    names=['tour', 'departement', 'commune_code', 'commune_nom', 'bureau', 'inscrits', 'votants', 'exprimes',
           'numero_candidat', 'nom_candidat', 'prenom_candidat', 'choix', 'voix'],
    dtype={'departement': str, 'commune_code': str, 'bureau': str},
    usecols=use_columns
)

stats_2007, choix_2007 = calculer_totaux(df_2007)

df_pres_2012 = pd.read_csv(
    'data/pres_2012.csv',
    sep=';',
    encoding='cp1252',
    names=['tour', 'departement', 'commune_code', 'commune_nom', '?', '??', 'bureau', 'inscrits', 'votants', 'exprimes',
           'numero_candidat', 'nom_candidat', 'prenom_candidat', 'choix', 'voix'],
    dtype={'departement': str, 'commune_code': str, 'bureau': str},
    usecols=use_columns
)
stats_2012, choix_2012 = calculer_totaux(df_pres_2012)


df_legi_2012 = pd.read_csv(
    'data/legi_2012.csv',
    sep=';',
    skiprows=18,
    names=['tour', 'departement', 'commune_code', 'commune_nom', '?', '??', 'bureau', 'inscrits', 'votants', 'exprimes',
           'numero_candidat', 'nom_candidat', 'prenom_candidat', 'choix', 'voix'],
    dtype={'departement': str, 'commune_code': str, 'bureau': str},
    usecols=use_columns
)
stats_legi_2012, choix_legi_2012 = calculer_totaux(df_legi_2012)


# statistiques tce
scores_tce = pd.DataFrame({
    'OUI_TCE': 100 * choix_2005[1]['OUI'] / stats_2005[1]['inscrits'],
    'NON_TCE': 100 * choix_2005[1]['NON'] / stats_2005[1]['inscrits']
})

# statistiques présidentielles 2012
nonistes_droite_2012 = ["LEPE", "DUPO"]
nonistes_gauche_2012 = ["MELE", "ARTH", "POUT"]
scores_pres_2012 = calculer_scores(stats_2012, choix_2012,
                                   nonistes_droite=nonistes_droite_2012, nonistes_gauche=nonistes_gauche_2012)

# statistiques présidentielles 2007
nonistes_droite_2007 = ["LEPE", "NIHO", "VILL"]
nonistes_gauche_2007 = ["BUFF", "BESA", "SCHI"]
scores_pres_2007 = calculer_scores(stats_2007, choix_2007,
                                   nonistes_droite=nonistes_droite_2007, nonistes_gauche=nonistes_gauche_2007)


# statistiques législatives 2012
# qui des divers droite ? Beaucoup doivent être nonistes
# sans doute moins le cas pour les divers gauche.
nonistes_droite_legislatives_2012 = ['FN', 'EXD']
nonistes_gauche_legislatives_2012 = ['FG', 'EXG']
scores_legi_2012 = calculer_scores(stats_legi_2012, choix_legi_2012,
                                   nonistes_droite=nonistes_droite_legislatives_2012,
                                   nonistes_gauche=nonistes_gauche_legislatives_2012)




df_communes = pd.concat([
    scores_tce,
    scores_pres_2012.rename(columns=lambda c: c + '_PRES_2012'),
    scores_pres_2007.rename(columns=lambda c: c + '_PRES_2007'),
    scores_legi_2012.rename(columns=lambda c: c + '_LEGI_2012')
], axis=1)

# Complete inseeinfos with special code from election results
# (Zx...).



listspecial = {dep+commune for (dep, commune), scores in df_communes.iterrows() if dep[0] == "Z"}

specialinseeinfos = pd.DataFrame({
        "departement"  : [special[0:2] for special in listspecial],
        "commune_code" : [special[2:] for special in listspecial],
        "listecodespostaux" : [convertSpecialToCodePostal(special) for special in listspecial]})\
    .set_index(["departement", "commune_code"]).ix[:, 'listecodespostaux']

df_communes = pd.concat([
    df_communes,
    inseeinfos,
], axis=1)

# a améliorer, on pourrait sortir directement du XML par exemple
communes = {dep+commune: scores[scores.notnull()].to_dict() for (dep, commune), scores in df_communes.iterrows()}

open("communes.json", 'w').write(json.dumps(communes, indent=4))
